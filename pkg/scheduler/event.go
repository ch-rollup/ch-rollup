package scheduler

import "fmt"

//go:generate go run github.com/alvaroloes/enumer -type=EventType -trimprefix=EventType -output=event_type_string.go

// EventType ...
type EventType uint8

const (
	// EventTypeRollUp ...
	EventTypeRollUp EventType = iota + 1
)

// Event ...
type Event struct {
	Type  EventType
	Error error
}

// String returns string representation of Event.
func (e Event) String() string {
	eventType := e.Type.String()

	if e.Error != nil {
		return fmt.Sprintf("%s was failed with error: %s", eventType, e.Error.Error())
	}

	return eventType
}
