package models

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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

	Metadata DiscMetadata

	Titles []*Title
}

type Track struct {
	TitleID int
	TrackID int
	Type    string
}

type Title struct {
	ID       int
	Duration time.Duration
	Chapters int

	VideoTracks    []*Track
	AudioTracks    []*Track
	SubtitleTracks []*Track

	Selected bool
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

	trackId, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}

	titleId, err := strconv.Atoi(fields[1])
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
