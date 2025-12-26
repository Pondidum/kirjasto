package goes

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var eventFactory = map[string]func() any{}

type EventDescriptor struct {
	AggregateID   uuid.UUID
	AggregateType string
	Sequence      int
	Timestamp     time.Time
	EventType     string

	json []byte
}

func (ed *EventDescriptor) Event() (any, error) {
	event, err := newEvent(ed.EventType)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(ed.json, &event); err != nil {
		return nil, err
	}

	return event, nil
}

func newEvent(eventType string) (any, error) {
	if factory, found := eventFactory[eventType]; found {
		return factory(), nil
	}

	return nil, fmt.Errorf("no factory for %s found", eventType)
}
