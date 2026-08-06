package arrange

import (
	"context"
	"strings"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/service"
)

type Service struct {
	Paths []string
}

func (s *Service) Run(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func New(config *config.Config) service.Service {

	mediaPaths := strings.Split(config.MediaStorage, ",")

	return &Service{
		Paths: mediaPaths,
	}
}
