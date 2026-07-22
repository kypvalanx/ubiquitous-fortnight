package kafka

import (
	"context"
	"encoding/json"

	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func (c Consumer) Close() error {
	return c.reader.Close()
}

func (c Consumer) ReadMessage(ctx context.Context) (events.Event, error) {
	message, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return events.Event{}, err
	}

	var event events.Event
	err = json.Unmarshal(message.Value, &event)
	if err != nil {
		return events.Event{}, err
	}

	return event, err
}

func NewConsumer(brokers []string, topic string, groupId string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers: brokers,
				Topic:   topic,
				GroupID: groupId,
			},
		),
	}
}
