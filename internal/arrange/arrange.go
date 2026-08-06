package arrange

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/kypvalanx/bluray-ripper/internal/service"
)

const digitFormat = "%02d"

type Service struct {
	Paths            []string
	consumer         kafka.Consumer
	ServiceName      string
	producer         kafka.Producer
	progressProducer kafka.Producer
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
		message := events.Event[models.ConvertedData]{}
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

		err = s.ArrangeTitles(message.Payload.ConvertedTitles)

		if err != nil {
			log.Printf("[%s Service] conversion error: %v", s.ServiceName, err)
		}

		event := events.Event[models.ArrangedData]{
			ID:            uuid.New().String(),
			Type:          "FilesArranged",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload:       models.ArrangedData{},
		}

		err1 := s.producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[%s Service] Kafka error: %v", s.ServiceName, err1)
		}
	}
}

func (s Service) ArrangeTitles(titles []models.ConvertedTitle) error {

	//allocatedBytes := make(map[string]uint64)
	for _, title := range titles {

		targetPath := GetMediaPath(title)
		//then select the drive
		mediaDrive := s.SelectMediaDrive(title.Type+"/"+targetPath[0], title.SizeInBytes)

		///	allocatedBytes[mediaDrive] += title.SizeInBytes

		filename := GetFileName(title)

		path := mediaDrive + "/" + title.Type + "/" + strings.Join(targetPath, "/") + filename

		err := os.Rename(title.TempFile, path)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetFileName(title models.ConvertedTitle) string {
	tokens := strings.Split(title.TempFile, ".")
	extension := tokens[len(tokens)-1]

	if title.Type == "Movies" {
		return CleanMediaName(title.Name) +
			" (" + title.Year +
			")." + extension
	} else if title.Type == "Shows" {
		return CleanMediaName(title.Name) +
			"S" + fmt.Sprintf(digitFormat, title.Season) +
			"E" + fmt.Sprintf(digitFormat, title.Episode) +
			"." + extension
	}
	return title.TempFile
}

func GetMediaPath(title models.ConvertedTitle) []string {
	titleNameTokens := []string{title.Name, "(" + title.Year + ")"}

	for _, tag := range title.MetaTags {
		titleNameTokens = append(titleNameTokens, "["+tag+"]")
	}

	join := strings.Join(titleNameTokens, " ")

	join = CleanMediaName(join)
	//prepend the type for the folder, but after /s have been removed
	join = title.Type + "/" + join

	mediaPath := []string{join}

	if title.Type == "Shows" {
		formatted := fmt.Sprintf(digitFormat, title.Season)
		mediaPath = append(mediaPath, "Season "+formatted)
	}
	return mediaPath
}

func CleanMediaName(join string) string {
	for _, old := range []string{"<", ">", ":", "\"", "/", "\\", "|", "?", "*"} {
		join = strings.ReplaceAll(join, old, "")
	}
	return join
}

func (s Service) SelectMediaDrive(folder string, size uint64) string {

	var highestPercent float64 = 0
	var selectedPath = ""
	//look for the existing show files if there's enough space OR the lowest usage
	for _, path := range s.Paths {

		usage, _ := GetDiskUsage(path)
		remainingBytes := usage.FreeBytes //- allocatedBytes[path]

		//if it doesn't fit, next
		if remainingBytes <= size {
			continue
		}

		exists, _ := FolderExists(strings.TrimSuffix(path, "/") + "/" + folder)

		if exists {
			//is there a folder already? let's keep a show together
			return path
		}
		percentFree := float64(remainingBytes) / float64(usage.TotalBytes) * 100

		if percentFree > highestPercent {
			highestPercent = percentFree
			selectedPath = path
		}
	}

	//the most empty drive by percentage
	return selectedPath
}

func FolderExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func New(cfg *config.Config) service.Service {

	mediaPaths := strings.Split(cfg.MediaStorage, ",")

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
		"disc.converted",
		"disc-arrange-worker",
	)

	return &Service{
		Paths:            mediaPaths,
		producer:         producer,
		consumer:         consumer,
		progressProducer: progressProducer,
	}
}
