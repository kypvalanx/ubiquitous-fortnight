package titleselector

import (
	"context"
	"fmt"
	"log"
	"strconv"
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

type TitleSelector struct {
	Config      *config.Config
	Producer    kafka.Producer
	Consumer    kafka.Consumer
	ServiceName string
	redis       *redis.Client
}

// New creates a new service listening on disc.metadata and producing success messages on disc.title.selected
func New(cfg *config.Config, redisClient *redis.Client) *TitleSelector {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.titles.selected",
	)
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		"disc.metadata",
		"title-selector-worker",
	)
	return &TitleSelector{
		Config:      cfg,
		Producer:    producer,
		Consumer:    consumer,
		redis:       redisClient,
		ServiceName: "Title Selector",
	}
}

func (t *TitleSelector) Run(ctx context.Context) error {
	log.Printf("[%s Service] Starting...\n", t.ServiceName)

	defer func(Consumer kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(t.Consumer)

	for {
		message := events.Event[models.DecoratedData]{}
		err := t.Consumer.ReadMessage(ctx, &message)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping %s service", t.ServiceName)
				return nil
			}

			log.Printf("[%s Service] Kafka error: %v", t.ServiceName, err)
			continue
		}
		log.Printf("[%s Service] Kafka message: %v", t.ServiceName, message)

		metaMap := make(map[int]tmdb.MovieDetailResult)

		for _, movieDetailResult := range message.Payload.MovieDetailResults {
			metaMap[movieDetailResult.MovieResult.ID] = movieDetailResult
		}

		highestRank := 0
		titleRankings := make(map[int]int)
		titleMetaMap := make(map[int]int)
		for _, title := range message.Payload.DiscInfo.Titles {
			rank := 0

			rank += 20 * isAtLeastXMinutes(title, 20*time.Minute)
			isWithinX, matchedMeta := isWithinXMinutesOfMetadata(title, 2*time.Minute, message.Payload.MovieDetailResults)
			titleMetaMap[title.ID] = matchedMeta
			rank += 50 * isWithinX
			rank += 20 * hasLargestResolution(title, t.LargestResolution(ctx, &message.Payload.DiscInfo.Titles))
			titleRankings[title.ID] = rank
			if highestRank < rank {
				highestRank = rank
			}
		}

		var rippableTitles []models.RippableTitle

		for _, title := range message.Payload.DiscInfo.Titles {
			if titleRankings[title.ID] == highestRank {
				metaId := titleMetaMap[title.ID]
				meta := metaMap[metaId]
				rippableTitles = append(rippableTitles,
					models.RippableTitle{
						ID:       title.ID,
						Type:     "Movie",
						Name:     meta.MovieDetails.Title,
						Filename: title.FileName,
					},
				)
			}
		}

		event := events.Event[models.RipRequest]{
			ID:            uuid.New().String(),
			Type:          "TitlesRanked",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload: models.RipRequest{
				Folder: message.CorrelationID,
				Titles: rippableTitles,
			},
		}

		err1 := t.Producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[%s Service] Kafka error: %v", t.ServiceName, err1)
		}
	}
}

func hasLargestResolution(title *models.Title, resolution string) int {
	for _, track := range title.VideoTracks {
		if track.Resolution == resolution {
			return 1
		}
	}
	return 0
}

func (t *TitleSelector) LargestResolution(ctx context.Context, titles *[]*models.Title) string {
	key := "largest:resolution:" + fmt.Sprintf("%p", titles)

	val, err := t.redis.Get(ctx, key).Result()

	if err != nil {
		return val
	}

	largest := 0
	largestString := ""
	for _, title := range *titles {
		for _, videoTrack := range title.VideoTracks {
			res := videoTrack.Resolution

			tokens := strings.Split(res, "x")

			width, err := strconv.Atoi(tokens[0])
			if err != nil {
				continue
			}
			if width > largest {
				largest = width
				largestString = res
			}
		}
	}

	t.redis.Set(ctx, key, largestString, 24*time.Hour)

	return largestString
}

func isWithinXMinutesOfMetadata(title *models.Title, duration time.Duration, results []tmdb.MovieDetailResult) (int, int) {
	for _, result := range results {
		difference := title.Duration - (time.Duration(result.MovieDetails.Runtime) * time.Minute)
		if difference.Abs() < duration {
			return 1, result.MovieDetails.ID
		}
	}
	return 0, -1
}

func isAtLeastXMinutes(title *models.Title, duration time.Duration) int {
	if title.Duration > duration {
		return 1
	}
	return 0
}
