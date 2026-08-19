package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
	ambiguousStorage "github.com/kypvalanx/bluray-ripper/internal/ambiguous-storage"
	"github.com/kypvalanx/bluray-ripper/internal/arrange"
	"github.com/kypvalanx/bluray-ripper/internal/cache"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/convert"
	"github.com/kypvalanx/bluray-ripper/internal/discdata"
	"github.com/kypvalanx/bluray-ripper/internal/metadata"
	"github.com/kypvalanx/bluray-ripper/internal/ripdisc"
	"github.com/kypvalanx/bluray-ripper/internal/service"
	"github.com/kypvalanx/bluray-ripper/internal/titleselector"
	"github.com/kypvalanx/bluray-ripper/internal/watcher"
	"github.com/redis/go-redis/v9"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: no .env file found: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var wg sync.WaitGroup

	redisdb, err := strconv.Atoi(GetRequiredConfig("REDIS_DB"))

	if err != nil {
		log.Fatalf("failed to parse REDIS_DB: %v", err)
	}

	cfg := &config.Config{
		OpticalDrive:    "/dev/sr0",
		Debug:           true,
		DryRun:          true,
		KafkaAddress:    GetRequiredConfig("KAFKA_BROKERS"),
		TMDBKey:         GetRequiredConfig("TMDB_API_KEY"),
		RedisAddr:       GetRequiredConfig("REDIS_ADDR"),
		RedisPass:       GetOptionalConfig("REDIS_PASSWORD"),
		RedisDB:         redisdb,
		RipCache:        GetRequiredConfig("RIP_CACHE"),
		ConvertCache:    GetOptionalConfig("CONVERT_CACHE"),
		MediaStorage:    GetOptionalConfig("MEDIA_STORAGE_CONTEXT"),
		MongoDbUri:      GetOptionalConfig("MONGO_DB_URI"),
		MongoDbDatabase: GetOptionalConfig("MONGO_DB_DATABASE"),
	}

	redisClient := cache.New(cfg)

	defer func(redisClient *redis.Client) {
		err := redisClient.Close()
		if err != nil {
			log.Printf("failed to close redis client: %v", err)
		}
	}(redisClient)

	if err := cache.Ping(context.Background(), redisClient); err != nil {
		log.Fatal(err)
	}

	services := []service.Service{
		watcher.New(cfg),
		discdata.New(cfg),
		metadata.New(cfg, redisClient),
		titleselector.New(cfg, redisClient),
		ripdisc.New(cfg),
		convert.New(cfg),
		arrange.New(cfg),
		ambiguousStorage.New(cfg),
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

func GetRequiredConfig(key string) string {
	value := os.Getenv(key)

	if value == "" {
		log.Fatal(key + " is not set")
	}
	return value
}
func GetOptionalConfig(key string) string {
	value := os.Getenv(key)

	return value
}
