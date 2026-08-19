package discdata

import (
	"cmp"
	"context"
	"errors"
	"log"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
)

type Service struct {
	Config      *config.Config
	Producer    kafka.Producer
	Consumer    kafka.Consumer
	ServiceName string
}

// New creates a watcher service that produces events on the disc.discdata topic and consumes disc.discovered events
func New(cfg *config.Config) *Service {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		kafka.DiscData,
	)

	const groupId = "discdata-worker"
	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		kafka.DiscDiscovered,
		groupId,
	)

	return &Service{
		Config:      cfg,
		Producer:    producer,
		Consumer:    consumer,
		ServiceName: "Disc Data",
	}
}

func (m *Service) Run(ctx context.Context) error {
	log.Printf("[%s Service] Starting...\n", m.ServiceName)

	defer func(Consumer kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(m.Consumer)

	for {
		message := events.Event[events.DiscDetected]{}
		err := m.Consumer.ReadMessage(ctx, &message)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping discdata service")
				return nil
			}

			log.Printf("[Discdata Service] Kafka error: %v", err)
			continue
		}
		log.Printf("[Discdata Service] Kafka message: %v", message)

		discInfo, err := m.GetDiscInfo()
		log.Printf("[Discdata Service] Disc info: %+v", discInfo)

		event := events.Event[models.DiscInfo]{
			ID:            uuid.New().String(),
			Type:          "DiscInfoParsed",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload:       *discInfo,
		}

		err1 := m.Producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[Metadata Service] Kafka error: %v", err1)
		}
	}
}

func (m *Service) GetDiscInfo() (*models.DiscInfo, error) {
	cmd := exec.Command(
		"makemkvcon",
		"-r",
		"info",
		"disc:0",
	)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return nil, err
	}

	return ParseMakeMKVOutput(string(output))
}

func ParseMakeMKVOutput(o string) (*models.DiscInfo, error) {
	lines := strings.Split(o, "\n")

	label := ""
	discType := ""

	titleMap := map[int]*models.Title{}
	trackMap := map[string]*models.Track{}

	for _, line := range lines {
		if strings.HasPrefix(line, "CINFO:") || strings.HasPrefix(line, "TINFO:") || strings.HasPrefix(line, "SINFO:") {
			info, err := models.ParseRowInfo(line)
			if err != nil {
				log.Printf("[Metadata Service] ParseMakeMKVOutput: %v on line %s", err, line)
				continue
			}

			value := strings.Trim(info.Value, `"`)
			switch info.Type {
			case "CINFO":
				switch info.Code {
				case 2:
					label = value
				case 1:
					discType = value
				}
			case "TINFO":
				title := getOrCreateTitle(titleMap, info.TitleID)
				switch info.Code {
				case 9:
					t, err := time.Parse("15:04:05", value)
					if err != nil {
						panic(err)
					}
					duration := time.Duration(t.Hour())*time.Hour +
						time.Duration(t.Minute())*time.Minute +
						time.Duration(t.Second())*time.Second
					title.Duration = duration
				case 8:

					chapters, err := strconv.Atoi(value)
					if err != nil {
						panic(err)
					}
					title.Chapters = chapters

				case 27:
					title.FileName = value
				case 16:
					title.Playlist = strings.HasSuffix(value, ".mpls")
				}
			case "SINFO":
				track := getOrCreateTrack(trackMap, info.TitleID, info.TrackID)
				switch info.Code {
				case 1:
					track.Type = value
				case 19:
					track.Resolution = value
				}
			}
		}
	}

	for key, track := range trackMap {
		titleID, _, err := parseKey(key)
		if err != nil {
			log.Printf("[Metadata Service] ParseMakeMKVOutput key parsing error: %v", err)
			continue
		}

		title := titleMap[titleID]

		switch track.Type {
		case "Audio":
			title.AudioTracks = append(title.AudioTracks, track)
		case "Video":
			title.VideoTracks = append(title.VideoTracks, track)
		case "Subtitles":
			title.SubtitleTracks = append(title.SubtitleTracks, track)
		}

	}

	titles := make([]*models.Title, 0, len(titleMap))
	for _, value := range titleMap {
		titles = append(titles, value)
	}

	slices.SortFunc(titles, func(a *models.Title, b *models.Title) int {
		return cmp.Compare(a.ID, b.ID)
	})

	return &models.DiscInfo{
		Label:    label,
		DiscType: discType,
		Titles:   titles,
	}, nil
}

//func setTitle(titleMap map[int]models.Title, id int, title models.Title) {
//	titleMap[id] = title
//}

func getOrCreateTitle(titleMap map[int]*models.Title, id int) *models.Title {
	title, exists := titleMap[id]

	if !exists {
		titleMap[id] = &models.Title{
			ID: id,
		}
		title = titleMap[id]
	}
	return title
}

func getOrCreateTrack(trackMap map[string]*models.Track, titleId int, trackId int) *models.Track {
	key := createKey(titleId, trackId)

	track, exists := trackMap[key]

	if !exists {
		trackMap[key] = &models.Track{
			TitleID: titleId,
			TrackID: trackId,
		}
		track = trackMap[key]
	}

	return track
}

func createKey(titleId int, trackId int) string {
	return "track:" + strconv.Itoa(titleId) + ":" + strconv.Itoa(trackId)
}

func parseKey(key string) (int, int, error) {
	split := strings.Split(key, ":")
	if len(split) != 3 {
		return 0, 0, errors.New("invalid key")
	}
	titleId, err := strconv.Atoi(split[1])
	if err != nil {
		return 0, 0, err
	}
	trackId, err := strconv.Atoi(split[2])
	if err != nil {
		return 0, 0, err
	}
	return titleId, trackId, nil
}

//func setTrack(trackMap map[int]map[int]models.Track, track models.Track) {
//	titleSlice, exists := trackMap[track.TitleID]
//
//	if !exists {
//		titleSlice = map[int]models.Track{}
//	}
//
//	titleSlice[track.TrackID] = track
//
//	trackMap[track.TitleID] = titleSlice
//}
