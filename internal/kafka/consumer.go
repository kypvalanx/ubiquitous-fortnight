package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadMessage(ctx context.Context, event any) error
	Close() error
	QuietClose()
}
type RealConsumer struct {
	reader *kafka.Reader
}

func (c RealConsumer) QuietClose() {
	err := c.Close()
	if err != nil {
		return
	}
}

func (c RealConsumer) Close() error {
	return c.reader.Close()
}

func (c RealConsumer) ReadMessage(ctx context.Context, event any) error {
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

func NewConsumer(brokers []string, topic TopicType, groupId string) Consumer {
	return &RealConsumer{
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers: brokers,
				Topic:   topic.Name,
				GroupID: groupId,
			},
		),
	}
}
