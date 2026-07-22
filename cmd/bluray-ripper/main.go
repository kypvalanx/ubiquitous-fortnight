package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/metadata"
	"github.com/kypvalanx/bluray-ripper/internal/service"
	"github.com/kypvalanx/bluray-ripper/internal/watcher"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var wg sync.WaitGroup

	cfg := &config.Config{
		OpticalDrive: "/dev/sr0",
		Debug:        true,
		DryRun:       true,
		KafkaAddress: "localhost:9092",
	}

	services := []service.Service{
		metadata.New(cfg),
		watcher.New(cfg),
	}

	for _, s := range services {
		wg.Add(1)
		go func(s service.Service) {
			defer wg.Done()
			if err := s.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("Service error: %v", err)
			}
		}(s)
	}
	<-ctx.Done()
	wg.Wait()
}
