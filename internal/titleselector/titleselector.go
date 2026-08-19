package titleselector

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/metadata"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/redis/go-redis/v9"
)

type TitleSelector struct {
	Config            *config.Config
	Producer          kafka.Producer
	AmbiguousProducer kafka.Producer
	Consumer          kafka.Consumer
	ServiceName       string
	redis             *redis.Client
}

// New creates a new service listening on disc.metadata and producing success messages on disc.title.selected
func New(cfg *config.Config, redisClient *redis.Client) *TitleSelector {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		kafka.DiscTitlesSelected,
	)
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		kafka.DiscMetadata,
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

		rankedTitles := t.RankCandidates(ctx, message.Payload.Candidates, message.Payload.DiscInfo)

		rippableTitles, ambiguousTitles, err := t.ResolveMatches(rankedTitles)

		fmt.Println(rippableTitles, ambiguousTitles, err)
		t.sendRipRequest(ctx, message)
	}
}

func (t *TitleSelector) sendRipRequest(ctx context.Context, message events.Event[models.DecoratedData]) {
	event := events.Event[models.RipRequest]{
		ID:            uuid.New().String(),
		Type:          "TitlesRanked",
		Timestamp:     time.Now(),
		CorrelationID: message.CorrelationID,
		Payload: models.RipRequest{
			Folder: message.CorrelationID,
		},
	}

	err1 := t.Producer.Send(ctx, event)

	if err1 != nil {
		log.Printf("[%s Service] Kafka error: %v", t.ServiceName, err1)
	}
}

func (t *TitleSelector) RankCandidates(ctx context.Context, candidates []models.MetadataCandidate, info models.DiscInfo) []models.MetadataMatch {
	candidateMap := make(map[string]models.MetadataCandidate)
	matches := []models.MetadataMatch{}

	largestResoution := t.LargestResolution(ctx, &info.Titles)
	season := ParseSeason(info)
	name := ParseName(info)

	for _, candidate := range candidates {
		key := candidate.Type + "-" + strconv.Itoa(candidate.ID)
		candidateMap[key] = candidate
		for _, title := range info.Titles {
			score := t.scoreMetadataMatch(title, candidate, models.MetadataMatchContext{
				LargestResolution: largestResoution,
				Season:            season,
				Name:              name,
			})

			if score >= 60 {
				matches = append(matches, models.MetadataMatch{
					TitleID:    title.ID,
					MetadataID: key,
					Score:      score,
				})
			}
		}
	}

	return matches
}

func ParseName(info models.DiscInfo) string {
	return metadata.CleanQuery(info.Label)
}

var seasonRegex = regexp.MustCompile(`(?i)\bseason[\s\-_]*(\d+)\b`)

func ParseSeason(info models.DiscInfo) int {
	discLabel := strings.ToLower(info.Label)

	matches := seasonRegex.FindStringSubmatch(discLabel)

	if matches == nil {
		return -1
	}

	match, err := strconv.Atoi(matches[0])

	if err != nil {
		return -1
	}

	return match
}

// returns rank 0 to 100
func (t *TitleSelector) scoreMetadataMatch(title *models.Title, metadata models.MetadataCandidate, matchContext models.MetadataMatchContext) int {
	rank := 0

	rank += 30 * nameMatch(matchContext.Name, metadata)
	rank += 40 * seasonMatches(matchContext.Season, metadata.SeasonNumber)
	rank += 50 * isWithinXMinutesOfMetadata(title, 2*time.Minute, metadata)
	rank += 20 * hasLargestResolution(title, matchContext.LargestResolution)
	return rank
}

func nameMatch(name string, candidate models.MetadataCandidate) int {
	if strings.EqualFold(
		metadata.CleanQuery(name),
		metadata.CleanQuery(candidate.Name)) {
		return 1
	}
	return 0
}

func seasonMatches(season int, number int) int {
	if season == -1 {
		return 0
	}
	if season == number {
		return 1
	}
	return -1
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
	if t.redis != nil {

		val, err := t.redis.Get(ctx, key).Result()

		if err != nil {
			return val
		}
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

	if t.redis != nil {
		t.redis.Set(ctx, key, largestString, 24*time.Hour)
	}

	return largestString
}

var minimumMatchScore = 70
var minimumScoreGap = 10

func (t *TitleSelector) ResolveMatches(matches []models.MetadataMatch) ([]models.MetadataMatch, []models.AmbiguousTitle, error) {

	candidatesByTitle := make(map[int][]models.MetadataMatch)

	for _, match := range matches {
		if match.Score < minimumMatchScore {
			continue
		}
		candidatesByTitle[match.TitleID] = append(candidatesByTitle[match.TitleID], match)
	}

	var resolved []models.MetadataMatch
	var ambiguous []models.AmbiguousTitle

	for _, candidates := range candidatesByTitle {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Score > candidates[j].Score
		})

		best := candidates[0]

		if len(candidates) > 1 {
			second := candidates[1]

			if best.Score-second.Score < minimumScoreGap {
				ambiguous = append(ambiguous, models.AmbiguousTitle{
					TitleID:    best.TitleID,
					Candidates: candidates,
				})
				continue
			}
		}
		resolved = append(resolved, best)
	}

	return resolved, ambiguous, nil
}

func isWithinXMinutesOfMetadata(title *models.Title, duration time.Duration, candidate models.MetadataCandidate) int {

	difference := title.Duration - (time.Duration(candidate.Runtime) * time.Minute)
	if difference.Abs() < duration {
		return 1
	}
	return 0
}

func isAtLeastXMinutes(title *models.Title, duration time.Duration) int {
	if title.Duration > duration {
		return 1
	}
	return 0
}
