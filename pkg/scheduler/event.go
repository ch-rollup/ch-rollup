package scheduler

import "fmt"

//go:generate go run github.com/alvaroloes/enumer -type=EventType -trimprefix=EventType -output=event_type_string.go

type EventType uint8

const (
	EventTypeRollUp EventType = iota + 1
)

type Event struct {
	Type  EventType
	Error error
}

func (e Event) String() string {
	eventType := e.Type.String()

	if e.Error != nil {
		return fmt.Sprintf("%s was failed with error: %s", eventType, e.Error.Error())
	}

	return eventType
}
