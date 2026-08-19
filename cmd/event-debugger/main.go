package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	kafka2 "github.com/kypvalanx/bluray-ripper/internal/kafka"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,    // Ctrl+C
		syscall.SIGTERM, // docker stop
	)

	defer stop()

	var wg sync.WaitGroup

	for _, topic := range kafka2.Topics {
		wg.Add(1)

		go func(topic kafka2.TopicType) {
			defer wg.Done()
			consumeTopic(ctx, topic)
		}(topic)
	}

	<-ctx.Done()

	log.Println("Shutdown Requested...")

	wg.Wait()

	log.Println("Shutdown Complete.")
}

func consumeTopic(ctx context.Context, topic kafka2.TopicType) {
	fmt.Println("topic start")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   string(topic),
		GroupID: "event-debugger",
	})

	defer reader.Close()

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("Stopping consumer for %s", topic)
				return
			}

			log.Printf("[%s] Kafka error: %v", topic, err)
			continue
		}

		log.Printf("[%s] %s", topic, string(message.Value))
	}
}
