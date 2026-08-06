package config

type Config struct {
	OpticalDrive string
	Debug        bool
	DryRun       bool
	KafkaAddress string
	TMDBKey      string
	RedisAddr    string
	RedisPass    string
	RedisDB      int
	RipCache     string
	ConvertCache string
	MediaStorage string
}
