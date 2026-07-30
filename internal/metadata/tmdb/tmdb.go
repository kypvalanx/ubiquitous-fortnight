package tmdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	redis      *redis.Client
}

func NewClient(apiKey string, redis *redis.Client) *Client {
	return &Client{
		apiKey:     apiKey,
		baseURL:    "https://api.themoviedb.org/3",
		httpClient: &http.Client{},
		redis:      redis,
	}
}

func (c *Client) SearchMovie(ctx context.Context, title string, year int) ([]MovieResult, error) {
	key := "tmbd:movie:" + strings.ToLower(title) + ":" + strconv.Itoa(year)

	value, err := c.redis.Get(ctx, key).Bytes()

	if err == nil {
		var movies []MovieResult
		if err := json.Unmarshal(value, &movies); err == nil {
			return movies, nil
		}
	}

	endpoint := fmt.Sprintf(
		"%s/search/movie",
		c.baseURL,
	)

	params := url.Values{}
	params.Set("api_key", c.apiKey)
	params.Set("query", title)

	if year > 0 {
		params.Set("year", fmt.Sprint(year))
	}

	resp, err := c.httpClient.Get(
		endpoint + "?" + params.Encode(),
	)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var result MovieSearchResponse

	err = json.NewDecoder(resp.Body).Decode(&result)

	bytes, _ := json.Marshal(result.Results)

	c.redis.Set(ctx, key, bytes, 24*time.Hour)

	return result.Results, err
}

func (c *Client) GetMovieDetails(ctx context.Context, id int) (*MovieDetails, error) {
	key := "tmbd:movie:details:" + strconv.Itoa(id)

	value, err := c.redis.Get(ctx, key).Bytes()

	if err == nil {
		var movies *MovieDetails
		if err := json.Unmarshal(value, &movies); err == nil {
			return movies, nil
		}
	}

	endpoint := fmt.Sprintf(
		"%s/movie/%d",
		c.baseURL,
		id,
	)

	params := url.Values{}
	params.Set("api_key", c.apiKey)

	resp, err := c.httpClient.Get(
		endpoint + "?" + params.Encode(),
	)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var result MovieDetails

	err = json.NewDecoder(resp.Body).Decode(&result)

	bytes, _ := json.Marshal(result)

	c.redis.Set(ctx, key, bytes, 24*time.Hour)

	return &result, err
}

func (c *Client) SearchMovieDetails(ctx context.Context, title string, year int) ([]MovieDetailResult, error) {

	movieSearchResponse, err := c.SearchMovie(ctx, title, year)

	if err != nil {
		return nil, err
	}

	movieDetailsResults := make([]MovieDetailResult, len(movieSearchResponse))

	for i, movieResult := range movieSearchResponse {
		movieDetails, err := c.GetMovieDetails(ctx, movieResult.ID)

		if err != nil {
			log.Printf("[Metadata] Error getting movie details: %v", err)
		}

		movieDetailsResults[i] = MovieDetailResult{
			MovieResult:  &movieResult,
			MovieDetails: movieDetails,
		}
	}

	return movieDetailsResults, nil
}
