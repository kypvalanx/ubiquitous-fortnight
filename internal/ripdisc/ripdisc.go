package ripdisc

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
)

type Service struct {
	config           *config.Config
	consumer         *kafka.Consumer
	producer         *kafka.Producer
	progressProducer *kafka.Producer
	ServiceName      string
}

func New(cfg *config.Config) *Service {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.ripped",
	)
	progressProducer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.rip.progress",
	)
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		"disc.titles.selected",
		"disc-rip-worker",
	)

	return &Service{
		config:           cfg,
		progressProducer: progressProducer,
		producer:         producer,
		consumer:         consumer,
		ServiceName:      "Rip Disc",
	}
}

func (s *Service) Run(ctx context.Context) error {
	log.Printf("[%s Service] Starting...\n", s.ServiceName)

	defer func(Consumer *kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(s.consumer)

	for {
		message := events.Event[models.RipRequest]{}
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

		err = s.RipTracks(ctx, message.Payload.Titles, message.CorrelationID)

		if err != nil {
			log.Printf("[%s Service] ripping error: %v", s.ServiceName, err)
		}

		titles := []models.ConvertableTitle{}

		for _, title := range message.Payload.Titles {
			titles = append(titles, models.ConvertableTitle{
				Filename: title.Filename,
			})
		}

		log.Printf("[%s Service] Tracks Ripped", s.ServiceName)

		event := events.Event[models.RippedData]{
			ID:            uuid.New().String(),
			Type:          "TracksRipped",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload: models.RippedData{
				Titles: titles,
			},
		}

		err1 := s.producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err1)
		}
	}
}

func (s *Service) RipTracks(ctx context.Context, tracks []models.RippableTitle, correlationID string) error {
	outputDir := fmt.Sprintf(s.config.RipCache, correlationID)

	outputDir, _ = filepath.Abs(outputDir)
	log.Printf("[%s Service] Creating directory: %s", s.ServiceName, outputDir)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating %s: %w", outputDir, err)
	}

	args := []string{"--robot",
		"--messages=-stdout",
		"--progress=-stdout", "mkv", "disc:0"}

	for _, track := range tracks {
		args = append(args, strconv.Itoa(track.ID))
	}

	args = append(args, outputDir)

	cmd := exec.Command("makemkvcon", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	//stdoutCh := events.StreamOutput(stdout)
	//stderrCh := events.StreamOutput(stderr)

	log.Printf("[%s Service] Ripping titles\n", s.ServiceName)
	if err := cmd.Start(); err != nil {
		return err
	}

	var wg sync.WaitGroup

	const workers = 4
	wg.Add(workers)

	lineCh := events.StreamOutput(stdout)
	for i := range workers {
		go func(workerId int) {
			defer wg.Done()

			for line := range lineCh {
				if line.Err != nil {
					log.Printf("worker %d error: %v\n", workerId, line.Err)
					continue
				}

				go s.handleMakeMKVLine(ctx, line.Line, correlationID)
			}
		}(i)
	}

	wg.Go(func() {

		for line := range events.StreamOutput(stderr) {
			if line.Err != nil {
				log.Println("stderr:", line.Err)
				continue
			}

			log.Println("stderr:", line.Line)
		}
	})

	// Wait for the process to exit
	if err := cmd.Wait(); err != nil {
		return err
	}

	// Now wait for stdout/stderr to drain
	wg.Wait()

	log.Printf("[%s Service] Finished ripping titles\n", s.ServiceName)
	return nil
}

func (s *Service) handleMakeMKVLine(ctx context.Context, line string, correlationID string) {
	tokens := strings.SplitN(line, ":", 2)

	if len(tokens) != 2 {
		return
	}

	switch tokens[0] {
	case "PRGC":
		subs := strings.Split(tokens[1], ",")
		progress := models.RipProgress{
			Status: strings.Trim(subs[2], `"`),
		}
		s.emitProgress(ctx, correlationID, progress)
	case "PRGV":
		subs := strings.Split(tokens[1], ",")

		if len(subs) < 3 {
			return
		}
		read, err := strconv.Atoi(subs[0])
		if err != nil {
			return
		}
		write, err := strconv.Atoi(subs[1])
		if err != nil {
			return
		}
		total, err := strconv.Atoi(subs[2])
		if err != nil {
			return
		}
		read = read * 100 / total
		write = write * 100 / total
		progress := models.RipProgress{
			Read:  read,
			Write: write,
		}
		s.emitProgress(ctx, correlationID, progress)
	}

}

func (s *Service) emitProgress(ctx context.Context, correlationID string, progress models.RipProgress) {

	event := events.Event[models.RipProgress]{
		ID:            uuid.New().String(),
		Type:          "RipProgress",
		Timestamp:     time.Now(),
		CorrelationID: correlationID,
		Payload:       progress,
	}

	err1 := s.progressProducer.Send(ctx, event)

	if err1 != nil {
		log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err1)
	}
}
