package tmdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func TestClient_SearchMovieDetails(t *testing.T) {
	_ = godotenv.Load()

	value := os.Getenv("TMDB_API_KEY")

	client := NewClient(value, nil)

	search, _ := client.SearchTVDetails(nil, "Battlestar Galactica", 0)

	fmt.Println(search)
}
