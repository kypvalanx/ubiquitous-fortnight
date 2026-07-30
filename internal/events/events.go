package events

import "time"

type Event[T any] struct {
	ID            string `json:"id"`
	Type          string
	Timestamp     time.Time
	CorrelationID string
	Payload       T
}

type DiscDetected struct {
	Device string
}
