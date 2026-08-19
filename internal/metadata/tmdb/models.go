package tmdb

type MovieSearchResponse struct {
	Page    int           `json:"page"`
	Results []MovieResult `json:"results"`
}

type MovieResult struct {
	ID            int     `json:"id"`
	Title         string  `json:"title"`
	OriginalTitle string  `json:"original_title"`
	ReleaseDate   string  `json:"release_date"`
	Overview      string  `json:"overview"`
	PosterPath    string  `json:"poster_path"`
	Popularity    float64 `json:"popularity"`
	VoteAverage   float64 `json:"vote_average"`
}

type MovieDetails struct {
	ID               int    `json:"id"`
	Title            string `json:"title"`
	OriginalTitle    string `json:"original_title"`
	ReleaseDate      string `json:"release_date"`
	Tagline          string `json:"tagline"`
	Runtime          int    `json:"runtime"`
	OriginalLanguage string `json:"original_language"`
}

type MovieDetailResult struct {
	MovieResult  *MovieResult  `json:"result"`
	MovieDetails *MovieDetails `json:"results"`
}

type TVSearchResponse struct {
	Page    int        `json:"page"`
	Results []TVResult `json:"results"`
}

type TVResult struct {
	Adult        bool   `json:"adult"`
	BackdropPath string `json:"backdrop_path"`
	GenreIds     []int  `json:"genre_ids"`
	ID           int    `json:"id"`
	//OriginCountry    string  `json:"origin_country"`
	OriginalLanguage string  `json:"original_language"`
	OriginalName     string  `json:"original_name"`
	Overview         string  `json:"overview"`
	Popularity       float64 `json:"popularity"`
	PosterPath       string  `json:"poster_path"`
	FirstAirDate     string  `json:"first_air_date"`
	Name             string  `json:"name"`
	VoteAverage      float64 `json:"vote_average"`
	VoteCount        float64 `json:"vote_count"`
}

type TVDetailResult struct {
	TVResult      *TVResult  `json:"result"`
	TVDetails     *TVDetails `json:"details"`
	SeasonDetails []SeasonDetails
}

type Season struct {
	AirDate      string  `json:"air_date"`
	EpisodeCount int     `json:"episode_count"`
	Id           int     `json:"id"`
	Name         string  `json:"name"`
	Overview     string  `json:"overview"`
	PosterPath   string  `json:"poster_path"`
	SeasonNumber int     `json:"season_number"`
	VoteAverage  float64 `json:"vote_average"`
}

type TVDetails struct {
	Adult            bool     `json:"adult"`
	BackdropPath     string   `json:"backdrop_path"`
	EpisodeRunTime   []int    `json:"episode_run_time"`
	FirstAirDate     string   `json:"first_air_date"`
	Homepage         string   `json:"homepage"`
	ID               int      `json:"id"`
	Name             string   `json:"name"`
	NumberOfEpisodes int      `json:"number_of_episodes"`
	NumberOfSeasons  int      `json:"number_of_seasons"`
	Overview         string   `json:"overview"`
	OriginalName     string   `json:"original_name"`
	Popularity       float64  `json:"popularity"`
	PosterPath       string   `json:"poster_path"`
	Seasons          []Season `json:"seasons"`
	Status           string   `json:"status"`
	Tagline          string   `json:"tagline"`
	Type             string   `json:"type"`
	VoteAverage      float64  `json:"vote_average"`
	VoteCount        float64  `json:"vote_count"`
}

type GuestStar struct{}

type Episode struct {
	AirDate        string      `json:"air_date"`
	EpisodeNumber  int         `json:"episode_number"`
	EpisodeType    string      `json:"episode_type"`
	Id             int         `json:"id"`
	Name           string      `json:"name"`
	Overview       string      `json:"overview"`
	ProductionCode string      `json:"production_code"`
	Runtime        int         `json:"runtime"`
	SeasonNumber   int         `json:"season_number"`
	ShowID         int         `json:"show_id"`
	StillPath      string      `json:"still_path"`
	VoteAverage    float64     `json:"vote_average"`
	VoteCount      float64     `json:"vote_count"`
	Crew           []Crew      `json:"crew"`
	GuestStars     []GuestStar `json:"guest_stars"`
}

type Network struct{}

type Crew struct{}

type SeasonDetails struct {
	ID           int       `json:"id"`
	AirDate      string    `json:"air_date"`
	Episodes     []Episode `json:"episodes"`
	Name         string    `json:"name"`
	Networks     []Network `json:"networks"`
	Overview     string    `json:"overview"`
	PosterPath   string    `json:"poster_path"`
	SeasonNumber int       `json:"season_number"`
	VoteAverage  float64   `json:"vote_average"`
}
