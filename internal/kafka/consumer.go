package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func (c Consumer) Close() error {
	return c.reader.Close()
}

func (c Consumer) ReadMessage(ctx context.Context, event any) error {
	message, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return err
	}

	err = json.Unmarshal(message.Value, event)
	if err != nil {
		return err
	}

	return err
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
