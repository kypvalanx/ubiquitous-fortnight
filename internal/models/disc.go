package models

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kypvalanx/bluray-ripper/internal/metadata/tmdb"
)

type DiscMetadata struct {
	Name        string
	Year        int
	IMDBID      string
	TMDBID      int
	Runtime     time.Duration
	CoverArtURL string
}

type DiscInfo struct {
	Label    string
	Drive    string
	DiscType string

	Titles []*Title
}

type Track struct {
	TitleID    int
	TrackID    int
	Type       string
	Resolution string
	FileName   string
}

//type VideoTrack struct {
//	Track
//}

type Title struct {
	ID       int
	Duration time.Duration
	Chapters int

	VideoTracks    []*Track
	AudioTracks    []*Track
	SubtitleTracks []*Track

	Selected bool
	FileName string
	Playlist bool
}

type RowInfo struct {
	Type    string
	TitleID int
	Code    int
	TrackID int
	Value   string
}

func ParseRowInfo(line string) (*RowInfo, error) {
	parts := strings.SplitN(line, ":", 2)

	switch parts[0] {
	case "CINFO":
		return parseCINFO(parts[1])
	case "TINFO":
		return parseTINFO(parts[1])
	case "SINFO":
		return parseSINFO(parts[1])
	}

	return nil, fmt.Errorf("invalid row info: %s", line)
}

func parseSINFO(s string) (*RowInfo, error) {

	fields := strings.SplitN(s, ",", 5)

	titleId, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}

	trackId, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}

	code, err := strconv.Atoi(fields[2])
	if err != nil {
		return nil, err
	}

	return &RowInfo{
		Type:    "SINFO",
		TrackID: trackId,
		TitleID: titleId,
		Code:    code,
		Value:   fields[4],
	}, nil
}

func parseTINFO(s string) (*RowInfo, error) {

	fields := strings.SplitN(s, ",", 4)

	titleId, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}

	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}

	return &RowInfo{
		Type:    "TINFO",
		TitleID: titleId,
		Code:    code,
		Value:   fields[3],
	}, nil
}

func parseCINFO(s string) (*RowInfo, error) {

	fields := strings.SplitN(s, ",", 3)

	code, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}

	return &RowInfo{
		Type:  "CINFO",
		Code:  code,
		Value: fields[2],
	}, nil
}

type DecoratedData struct {
	DiscInfo           DiscInfo
	MovieDetailResults []tmdb.MovieDetailResult
	Candidates         []MetadataCandidate
}

type RipRequest struct {
	Folder  string
	Matches []MetadataMatch
}

type RippedData struct {
	Titles []ConvertableTitle
}

type RippableTitle struct {
	ID       int
	Type     string
	Name     string
	Filename string
	Year     string
	MetaTags []string
	Season   int
	Episode  int
}

type RipProgress struct {
	Read   int
	Write  int
	Status string
}

type AmbiguousTitle struct {
	TitleID    int
	Candidates []MetadataMatch
}

type EncodeProgress struct {
	Raw string
}

type ConvertableTitle struct {
	Filename string
	Name     string
	Year     string
	MetaTags []string
	Type     string
	Season   int
	Episode  int
}

type ConvertedData struct {
	ConvertedTitles []ConvertedTitle
}

type ConvertedTitle struct {
	Name        string
	Year        string
	MetaTags    []string
	Season      int
	Type        string
	SizeInBytes uint64
	TempFile    string
	Episode     int
}

type ArrangedData struct {
}

type MetadataCandidate struct {
	Name             string
	Type             string
	ID               int
	Runtime          int
	EpisodeNumber    int
	SeasonNumber     int
	EpisodeTitle     string
	EpisodeType      string
	OriginalLanguage string
	EpisodeID        int
}

type MetadataMatch struct {
	TitleID    int
	MetadataID string
	Score      int
}

type MetadataMatchContext struct {
	LargestResolution string
	Season            int
	Name              string
}
