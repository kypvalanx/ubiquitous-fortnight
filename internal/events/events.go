package events

import "time"

type Event struct {
	ID            string `json:"id"`
	Type          string
	Timestamp     time.Time
	CorrelationID string
	Payload       interface{}
}

type DiscDetected struct {
	Device string
}
