package ambiguous_storage

import (
	"context"
	"log"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/kypvalanx/bluray-ripper/internal/service"
)

type Service struct {
	ServiceName              string
	consumer                 kafka.Consumer
	ambiguousTitleRepository AmbiguousTitleRepository
}

func New(cfg *config.Config) service.Service {
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		kafka.DiscTitlesAmbiguous,
		"store-candidates",
	)

	repo := NewAmbiguousTitleRepository(cfg)

	return &Service{
		ServiceName:              "StoreAmbiguousTitles",
		consumer:                 consumer,
		ambiguousTitleRepository: repo,
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

		s.ambiguousTitleRepository.Write(message)
	}

}
