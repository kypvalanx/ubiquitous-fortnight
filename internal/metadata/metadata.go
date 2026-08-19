package metadata

import (
	"context"
	"log"
	"regexp"
	"strings"
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
		kafka.DiscMetadata,
	)

	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		kafka.DiscData,
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

		candidates, _ := s.GetMetadataCandidates(&discInfo)

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
				MovieDetailResults: response, //TODO REMOVE
				Candidates:         candidates,
			},
		}

		err1 := s.Producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[Metadata Service] Kafka error: %v", err1)
		}
	}
}

func (s Service) GetMetadataCandidates(info *models.DiscInfo) ([]models.MetadataCandidate, error) {
	cleanQuery := CleanQuery(info.Label)

	movieDetails, err := s.TMDBClient.SearchMovieDetails(context.Background(), cleanQuery, 0)

	if err != nil {
		return nil, err
	}

	var candidates []models.MetadataCandidate
	for _, movieDetails := range movieDetails {
		//fmt.Println(movieDetails)
		candidates = append(candidates, models.MetadataCandidate{
			Name:             movieDetails.MovieResult.Title,
			Type:             "Movies",
			ID:               movieDetails.MovieDetails.ID,
			Runtime:          movieDetails.MovieDetails.Runtime,
			OriginalLanguage: movieDetails.MovieDetails.OriginalLanguage,
		})
	}

	tvDetails, err := s.TMDBClient.SearchTVDetails(context.Background(), cleanQuery, 0)

	for _, show := range tvDetails {
		for _, season := range show.SeasonDetails {
			for _, episode := range season.Episodes {
				candidates = append(candidates, models.MetadataCandidate{
					Name:             show.TVResult.Name,
					Type:             "Shows",
					ID:               show.TVResult.ID,
					Runtime:          episode.Runtime,
					EpisodeID:        episode.Id,
					EpisodeNumber:    episode.EpisodeNumber,
					SeasonNumber:     episode.SeasonNumber,
					EpisodeTitle:     episode.Name,
					EpisodeType:      episode.EpisodeType,
					OriginalLanguage: show.TVResult.OriginalLanguage,
				})
			}
		}
	}

	if err != nil {
		return nil, err
	}
	return candidates, nil
}

var discSuffixRegex = regexp.MustCompile(`[\s\(\[\-_]*(disc)[\s\-_]*\d+[\)\]]*\s*$`)
var seasonRegex = regexp.MustCompile(`(?i)\bseason[\s\-_]*\d+\b`)

func CleanQuery(label string) string {
	label = strings.ToLower(label)
	label = strings.Replace(label, "_", " ", -1)
	label = discSuffixRegex.ReplaceAllString(label, "")
	label = seasonRegex.ReplaceAllString(label, "")
	return label
}
