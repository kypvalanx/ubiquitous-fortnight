package makemkv

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/models"
)

type Service struct {
	Config *config.Config
}

func New(cfg *config.Config) *Service {
	return &Service{
		Config: cfg,
	}
}

func (s *Service) GetDiscInfo() (*models.DiscInfo, error) {
	cmd := exec.Command(
		"makemkvcon",
		"-r",
		"info",
		"disc:0",
	)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return nil, err
	}

	return s.ParseMakeMKVOutput(string(output))
}

func (s *Service) ParseMakeMKVOutput(o string) (*models.DiscInfo, error) {
	lines := strings.Split(o, "\n")

	for _, line := range lines {
		fmt.Println(line)
	}

	return &models.DiscInfo{}, nil
}
