package cache

import (
	"context"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/redis/go-redis/v9"
)

func New(cfg *config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       cfg.RedisDB,
	})
}

func Ping(ctx context.Context, client *redis.Client) error {
	return client.Ping(ctx).Err()
}
