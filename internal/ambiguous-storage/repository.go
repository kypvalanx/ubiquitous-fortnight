package ambiguous_storage

import (
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/events"
	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/kypvalanx/bluray-ripper/internal/mongo"
)

type AmbiguousTitleRepository interface {
	Write(message events.Event[models.ConvertedData])
}

type AmbiguousTitleRepositoryImpl struct {
	repository *mongo.Repository
}

func (a AmbiguousTitleRepositoryImpl) Write(message events.Event[models.ConvertedData]) {
	a.repository.Write(mongo.Document{
		CorrelationID: message.CorrelationID,
		Type:          message.Type,
		Payload:       message.Payload,
		Status:        "AMBIGUOUS",
	})
}

func NewAmbiguousTitleRepository(cfg *config.Config) AmbiguousTitleRepository {
	repository := mongo.NewRepository(mongo.RepositoryConfig{
		MongoDbDatabase:   cfg.MongoDbDatabase,
		MongoDbCollection: "ambiguous_titles",
		MongoDbUri:        cfg.MongoDbUri,
	})

	return AmbiguousTitleRepositoryImpl{
		repository: repository,
	}
}
