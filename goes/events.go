package goes

import "fmt"

var eventFactory = map[string]func() any{}

func newEvent(eventType string) (any, error) {
	if factory, found := eventFactory[eventType]; found {
		return factory(), nil
	}

	return nil, fmt.Errorf("no factory for %s found", eventType)
}
