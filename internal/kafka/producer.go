package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Send(ctx context.Context, message any) error
}

type RealProducer struct {
	writer *kafka.Writer
}

func NewProducer(address string, topic string) Producer {
	return &RealProducer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(address),
			Topic: topic,
		},
	}
}

func (p *RealProducer) Send(ctx context.Context, message any) error {
	data, err := json.Marshal(message)

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

type MockProducer struct{}

func (m MockProducer) Send(ctx context.Context, message any) error {
	fmt.Println(message)
	return nil
}

func NewMockProducer(address string, topic string) Producer {
	return &MockProducer{}
}
