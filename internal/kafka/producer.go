package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(address string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(address),
			Topic: topic,
		},
	}
}

func (p *Producer) Send(ctx context.Context, event any) error {
	data, err := json.Marshal(event)

	if err != nil {
		return err
	}

	return p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Value: data,
		},
	)
}
