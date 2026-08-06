package metadata

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/metadata/tmdb"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	Config      *config.Config
	Producer    kafka.Producer
	Consumer    kafka.Consumer
	TMDBClient  *tmdb.Client
	ServiceName string
}

func New(cfg *config.Config, client *redis.Client) *Service {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.metadata",
	)

	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		"disc.discdata",
		"metadata-worker",
	)

	tmdbClient := tmdb.NewClient(cfg.TMDBKey, client)

	return &Service{
		Config:      cfg,
		Producer:    producer,
		Consumer:    consumer,
		TMDBClient:  tmdbClient,
		ServiceName: "Metadata",
	}
}

func (s Service) Run(ctx context.Context) error {
	log.Printf("[%s Service] Starting...\n", s.ServiceName)

	defer func(Consumer kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(s.Consumer)

	for {
		message := events.Event[models.DiscInfo]{}
		err := s.Consumer.ReadMessage(ctx, &message)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping metadata service")
				return nil
			}

			log.Printf("[Metadata Service] Kafka error: %v", err)
			continue
		}
		log.Printf("[Metadata Service] Kafka message: %v", message)

		discInfo := message.Payload

		response, err := s.TMDBClient.SearchMovieDetails(ctx, discInfo.Label, 0)
		if err != nil {
			log.Printf("[Metadata Service] TMDB error: %v", err)
		}

		log.Printf("[Metadata Service] TMDB response: %v", response)

		event := events.Event[models.DecoratedData]{
			ID:            uuid.New().String(),
			Type:          "MetaDataRetrieved",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload: models.DecoratedData{
				DiscInfo:           discInfo,
				MovieDetailResults: response,
			},
		}

		err1 := s.Producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[Metadata Service] Kafka error: %v", err1)
		}
	}
}
