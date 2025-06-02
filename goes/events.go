package goes

import (
	"encoding/json"
	"fmt"
)

var eventFactory = map[string]func() any{}

func newEvent(eventType string) (any, error) {
	if factory, found := eventFactory[eventType]; found {
		return factory(), nil
	}

	return nil, fmt.Errorf("no factory for %s found", eventType)
}

func eventFromJson(eventType string, eventJson []byte) (any, error) {
	event, err := newEvent(eventType)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(eventJson, &event); err != nil {
		return nil, err
	}

	return event, nil
}
