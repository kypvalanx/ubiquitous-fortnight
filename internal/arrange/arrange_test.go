package arrange

import (
	"testing"

	"github.com/kypvalanx/bluray-ripper/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestGetFolderShow(t *testing.T) {
	path := GetMediaPath(models.ConvertedTitle{
		Name: "Star Trek",
		Year: "1968",
		MetaTags: []string{
			"tmdb-678",
		},
		Season: 1,
		Type:   "Shows",
	})

	expected := []string{"Star Trek (1968) [tmdb-678]", "Season 01"}
	assert.Equal(t, expected, path, "the paths should match")
}

func TestGetFolderMovie(t *testing.T) {
	path := GetMediaPath(models.ConvertedTitle{
		Name: "Star Trek: The Motion Picture",
		Year: "1987",
		MetaTags: []string{
			"tmdb-6783",
		},
		Season: 1, //should be ignored
		Type:   "Movies",
	})

	expected := []string{"Star Trek The Motion Picture (1987) [tmdb-6783]"}
	assert.Equal(t, expected, path, "the paths should match")
}

func TestGetFolderMovieNoMetaTags(t *testing.T) {
	path := GetMediaPath(models.ConvertedTitle{
		Name:   "Star Trek: The Motion Picture",
		Year:   "1987",
		Season: 1, //should be ignored
		Type:   "Movies",
	})

	expected := []string{"Star Trek The Motion Picture (1987)"}
	assert.Equal(t, expected, path, "the paths should match")
}
