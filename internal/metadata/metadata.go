package metadata

import (
	"context"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/kypvalanx/bluray-ripper/internal/models"
)

type Metadata struct {
	Config   *config.Config
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

func New(cfg *config.Config) *Metadata {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.metadata",
	)

	consumer := kafka.NewConsumer(
		[]string{cfg.KafkaAddress},
		"disc.discovered",
		"metadata-worker",
	)

	return &Metadata{
		Config:   cfg,
		Producer: producer,
		Consumer: consumer,
	}
}

func (m *Metadata) Run(ctx context.Context) error {
	log.Println("Starting metadata service")

	defer func(Consumer *kafka.Consumer) {
		err := Consumer.Close()
		if err != nil {
			return
		}
	}(m.Consumer)

	for {
		message, err := m.Consumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping metadata service")
				return nil
			}

			log.Printf("[Metadata Service] Kafka error: %v", err)
			continue
		}
		log.Printf("[Metadata Service] Kafka message: %v", message)

		discInfo, err := m.GetDiscInfo()
		log.Printf("[Metadata Service] Disc info: %+v", discInfo)

		event := events.Event{
			ID:            uuid.New().String(),
			Type:          "DiscInfoParsed",
			Timestamp:     time.Now(),
			CorrelationID: message.CorrelationID,
			Payload:       discInfo,
		}

		err1 := m.Producer.Send(ctx, event)

		if err1 != nil {
			log.Printf("[Metadata Service] Kafka error: %v", err1)
		}
	}

	//TODO implement me
	//panic("implement me")
}

func (m *Metadata) GetDiscInfo() (*models.DiscInfo, error) {
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

	return m.ParseMakeMKVOutput(string(output))
}

func (m *Metadata) ParseMakeMKVOutput(o string) (*models.DiscInfo, error) {
	lines := strings.Split(o, "\n")

	label := ""

	titleMap := map[int]models.Title{}
	trackMap := map[int]map[int]models.Track{}

	for _, line := range lines {
		if strings.HasPrefix(line, "CINFO:") || strings.HasPrefix(line, "TINFO:") || strings.HasPrefix(line, "SINFO:") {
			info, err := models.ParseRowInfo(line)
			if err != nil {
				log.Printf("[Metadata Service] ParseMakeMKVOutput: %v on line %s", err, line)
				continue
			}

			value := strings.Trim(info.Value, `"`)
			if info.Type == "CINFO" {
				//disc title
				if info.Code == 2 {
					label = value
				} else if info.Code == 32 {
					//disc name
				}
			} else if info.Type == "TINFO" {
				title := getOrCreateTitle(titleMap, info.TitleID)

				switch {
				case info.Code == 9:
					t, err := time.Parse("15:04:05", value)
					if err != nil {
						panic(err)
					}
					duration := time.Duration(t.Hour())*time.Hour +
						time.Duration(t.Minute())*time.Minute +
						time.Duration(t.Second())*time.Second
					title.Duration = duration
				case info.Code == 8:

					chapters, err := strconv.Atoi(value)
					if err != nil {
						panic(err)
					}
					title.Chapters = chapters
				}
				setTitle(titleMap, info.TitleID, title)
			} else if info.Type == "SINFO" {
				track := getOrCreateTrack(trackMap, info.TitleID, info.TrackID)

				switch info.Code {
				case 1:
					track.Type = value
				}
				setTrack(trackMap, track)
			}
		}
	}

	for titleID, title := range titleMap {
		trackSlice := trackMap[titleID]

		for _, track := range trackSlice {
			switch track.Type {
			case "Audio":
				title.AudioTracks = append(title.AudioTracks, &track)
			case "Video":
				title.VideoTracks = append(title.VideoTracks, &track)
			case "Subtitles":
				title.SubtitleTracks = append(title.SubtitleTracks, &track)
			}
		}

		titleMap[titleID] = title
	}

	titles := make([]*models.Title, 0, len(titleMap))
	for _, value := range titleMap {
		titles = append(titles, &value)
	}

	return &models.DiscInfo{
		Label:  label,
		Titles: titles,
	}, nil
}

func setTitle(titleMap map[int]models.Title, id int, title models.Title) {
	titleMap[id] = title
}

func getOrCreateTitle(titleMap map[int]models.Title, id int) models.Title {
	title, exists := titleMap[id]

	if !exists {
		title = models.Title{
			ID: id,
		}
		titleMap[id] = title
	}
	return title
}

func getOrCreateTrack(trackMap map[int]map[int]models.Track, titleId int, trackId int) models.Track {
	titleSlice, exists := trackMap[titleId]

	if !exists {
		titleSlice = map[int]models.Track{}
	}

	track, exists := titleSlice[trackId]

	if !exists {
		track = models.Track{
			TitleID: titleId,
			TrackID: trackId,
		}
		titleSlice[trackId] = track
	}

	trackMap[titleId] = titleSlice
	return track
}

func setTrack(trackMap map[int]map[int]models.Track, track models.Track) {
	titleSlice, exists := trackMap[track.TitleID]

	if !exists {
		titleSlice = map[int]models.Track{}
	}

	titleSlice[track.TrackID] = track

	trackMap[track.TitleID] = titleSlice
}
