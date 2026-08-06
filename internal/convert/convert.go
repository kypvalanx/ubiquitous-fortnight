package convert

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/commander"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/kypvalanx/bluray-ripper/internal/service"
)

type Service struct {
	config           *config.Config
	consumer         kafka.Consumer
	producer         kafka.Producer
	progressProducer kafka.Producer
	commander        commander.Commander
	ServiceName      string
}

func New(cfg *config.Config) service.Service {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.converted",
	)
	progressProducer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.convert.progress",
	)
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		"disc.ripped",
		"disc-convert-worker",
	)

	c := commander.New()

	return &Service{
		config:           cfg,
		producer:         producer,
		consumer:         consumer,
		commander:        c,
		progressProducer: progressProducer,
		ServiceName:      "Convert Disc",
	}
}

func (s Service) Run(ctx context.Context) error {
	log.Printf("[%s Service] Starting...\n", s.ServiceName)

	defer func(Consumer kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(s.consumer)

	for {
		message := events.Event[models.RippedData]{}
		err := s.consumer.ReadMessage(ctx, &message)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping %s service", s.ServiceName)
				return nil
			}

			log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err)
			continue
		}
		log.Printf("[%s Service] Kafka message: %v", s.ServiceName, message)

		err = s.ConvertTitles(ctx, message.Payload.Titles, message.CorrelationID)

		if err != nil {
			log.Printf("[%s Service] conversion error: %v", s.ServiceName, err)
		}

		event := events.Event[models.ConvertedData]{
			ID:            uuid.New().String(),
			Type:          "TracksRipped",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload:       models.ConvertedData{},
		}

		err1 := s.producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err1)
		}
	}
}

func (s Service) ConvertTitles(ctx context.Context, titles []models.ConvertableTitle, correlationID string) ([]models.ConvertedTitle, error) {
	outputDir := fmt.Sprintf(s.config.ConvertCache, correlationID)
	inputDir := fmt.Sprintf(s.config.RipCache, correlationID)

	outputDir, _ = filepath.Abs(outputDir)
	log.Printf("[%s Service] Creating directory: %s", s.ServiceName, outputDir)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", outputDir, err)
	}

	var convertedTitles []models.ConvertedTitle

	for _, title := range titles {
		preset, err := GetPreset(PresetDefault)
		if err != nil {
			log.Printf("[%s Service] Preset lookup error: %v", s.ServiceName, err)
			continue
		}

		inputFile := filepath.Join(inputDir, title.Filename)
		outputFile := filepath.Join(outputDir, title.Filename)

		args := []string{
			"-i", inputFile,
			"-o", outputFile,
		}

		args = append(args, preset.Args...)

		cmd := s.commander.CommandContext(
			ctx,
			"HandBrakeCLI",
			args...,
		)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		monitorCmd := s.commander.Command("nvidia-smi", "dmon", "-s", "pucvmet", "-d", "1")

		monitorStdOut, err := monitorCmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		monitorErrOut, err := monitorCmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		monitorStdOutCh := events.StreamOutput(monitorStdOut)
		monitorErrCh := events.StreamOutput(monitorErrOut)

		stdoutCh := events.StreamOutput(stdout)
		stderrCh := events.StreamOutput(stderr)

		if err := cmd.Start(); err != nil {
			return nil, err
		}

		stdoutOpen := true
		stderrOpen := true
		monitorStdOutOpen := true
		monitorErrOutOpen := true

		for stderrOpen || stdoutOpen || monitorStdOutOpen || monitorErrOutOpen {
			select {
			case line, ok := <-stdoutCh:
				if !ok {
					stdoutOpen = false
					continue
				}

				if line.Err != nil {
					log.Println("stdout:", line.Err)
					continue
				}

				s.handleHandbrakeLine(ctx, line.Line, correlationID)

			case line, ok := <-stderrCh:
				if !ok {
					stderrOpen = false
					continue
				}
				if line.Err != nil {
					log.Println("stderr:", line.Err)
					continue
				}
				log.Println("stderr:", line.Line)
			case line, ok := <-monitorStdOutCh:
				if !ok {
					monitorStdOutOpen = false
					continue
				}

				if line.Err != nil {
					log.Println("stdout:", line.Err)
					continue
				}

				s.handleHandbrakeLine(ctx, line.Line, correlationID)

			case line, ok := <-monitorErrCh:
				if !ok {
					monitorErrOutOpen = false
					continue
				}
				if line.Err != nil {
					log.Println("stderr:", line.Err)
					continue
				}
				log.Println("stderr:", line.Line)
			}
		}

		if err := cmd.Wait(); err != nil {
			return nil, err
		}
		fi, err := os.Stat(outputFile)
		if err != nil {
			return nil, err
		}
		// get the size
		size := fi.Size()

		convertedTitles = append(convertedTitles, models.ConvertedTitle{
			TempFile:    outputFile,
			Name:        title.Name,
			Year:        title.Year,
			MetaTags:    title.MetaTags,
			Type:        title.Type,
			SizeInBytes: uint64(size),
			Season:      title.Season,
			Episode:     title.Episode,
		})
	}

	return convertedTitles, nil
}

func (s Service) handleHandbrakeLine(ctx context.Context, line string, correlationID string) {
	log.Println(line)
	event := events.Event[models.EncodeProgress]{
		ID:            uuid.New().String(),
		Type:          "EncodeProgress",
		Timestamp:     time.Now(),
		CorrelationID: correlationID,
		Payload: models.EncodeProgress{
			Raw: line,
		},
	}

	err1 := s.progressProducer.Send(ctx, event)

	if err1 != nil {
		log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err1)
	}
}
