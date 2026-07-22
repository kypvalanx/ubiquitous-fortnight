package watcher

import (
	"context"
	"log"
	"os/exec"
	"time"

	"github.com/google/uuid"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/kafka"
)

type Watcher struct {
	Config      *config.Config
	Producer    *kafka.Producer
	discPresent bool
}

func New(
	cfg *config.Config,
) *Watcher {
	producer := kafka.NewProducer(
		cfg.KafkaAddress,
		"disc.discovered",
	)

	return &Watcher{
		Config:   cfg,
		Producer: producer,
	}
}

func (w *Watcher) Run(ctx context.Context) error {
	log.Println("Starting Bluray watcher")

	for {
		if ctx.Err() != nil {
			log.Printf("Stopping watcher service")
			return nil
		}

		present := w.DiscPresent()

		switch {
		case present && !w.discPresent:
			log.Println("Disc Detected")
			if err := w.EmitDiscDetected(); err != nil {
				return err
			}

		case !present && w.discPresent:
			log.Println("Disc Removed")

		}

		w.discPresent = present

		time.Sleep(5 * time.Second)
	}
}

func (w *Watcher) EmitDiscDetected() error {

	event := events.Event{
		ID:            uuid.New().String(),
		Type:          "DiscDetected",
		Timestamp:     time.Now(),
		CorrelationID: uuid.New().String(),
		Payload: events.DiscDetected{
			Device: w.Config.OpticalDrive,
		},
	}

	err := w.Producer.Send(
		context.Background(),
		event,
	)

	if err != nil {
		return err
	}

	return nil
}

func (w *Watcher) DiscPresent() bool {
	cmd := exec.Command(
		"blkid",
		w.Config.OpticalDrive,
	)

	err := cmd.Run()

	return err == nil
}

func (w *Watcher) GetDiscLabel() (string, error) {
	cmd := exec.Command(
		"blkid",
		w.Config.OpticalDrive,
	)

	info, err := cmd.CombinedOutput()

	if err != nil {
		return "", err
	}

	return string(info), nil

}
