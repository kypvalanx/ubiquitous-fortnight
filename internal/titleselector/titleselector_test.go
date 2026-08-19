package titleselector

import (
	"fmt"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/kypvalanx/bluray-ripper/internal/config"
	"github.com/kypvalanx/bluray-ripper/internal/discdata"
	"github.com/kypvalanx/bluray-ripper/internal/metadata"
)

func TestSelectTVShow(t *testing.T) {
	_ = godotenv.Load()

	apiKey := os.Getenv("TMDB_API_KEY")

	data, err := os.ReadFile("../discdata/testdata/bluray/GG_S1_D2.txt")
	if err != nil {
		t.Fatal(err)
	}

	discInfo, err := discdata.ParseMakeMKVOutput(string(data))

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%+v\n", discInfo)

	cfg := config.Config{
		TMDBKey: apiKey,
	}
	medadataService := metadata.New(&cfg, nil)
	candidates, err := medadataService.GetMetadataCandidates(discInfo)
	if err != nil {
		fmt.Println(err)
		return
	}

	titleSelector := New(&cfg, nil)

	matches := titleSelector.RankCandidates(nil, candidates, *discInfo)

	//decoratedData := models.DecoratedData{
	//	DiscInfo:   *discInfo,
	//	Candidates: candidates,
	//}

	rippable, ambiguous, err := titleSelector.ResolveMatches(matches)
	//fmt.Printf("%+v\n", decoratedData)
	fmt.Println(rippable, ambiguous, err)
}
