package tmdb

type MovieSearchResponse struct {
	Page    int           `json:"page"`
	Results []MovieResult `json:"results"`
}

type MovieResult struct {
	ID int `json:"id"`

	Title string `json:"title"`

	OriginalTitle string `json:"original_title"`

	ReleaseDate string `json:"release_date"`

	Overview string `json:"overview"`

	PosterPath string `json:"poster_path"`

	Popularity float64 `json:"popularity"`

	VoteAverage float64 `json:"vote_average"`
}

type MovieDetails struct {
	ID            int    `json:"id"`
	Title         string `json:"title"`
	OriginalTitle string `json:"original_title"`
	ReleaseDate   string `json:"release_date"`
	Tagline       string `json:"tagline"`
	Runtime       int    `json:"runtime"`
}

type MovieDetailResult struct {
	MovieResult  *MovieResult  `json:"result"`
	MovieDetails *MovieDetails `json:"results"`
}
