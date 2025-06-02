package goes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type AggregateState struct {
	id       uuid.UUID
	sequence int

	handlers map[string]func(event any) error

	pendingEvents []EventDescriptor
}

func NewAggregateState() *AggregateState {
	return &AggregateState{
		sequence: -1,
		handlers: map[string]func(event any) error{},
	}
}

func (a *AggregateState) ID() uuid.UUID {
	return a.id
}

func Register[TEvent any](state *AggregateState, handler func(event TEvent)) {
	name := reflect.TypeOf(*new(TEvent)).Name()

	state.handlers[name] = func(event any) error {

		switch e := event.(type) {
		case TEvent:
			handler(e)
		case *TEvent:
			handler(*e)
		default:
			return fmt.Errorf("unable to handle %T", e)
		}

		return nil
	}

	eventFactory[name] = func() any {
		return new(TEvent)
	}
}

func Apply[TEvent any](state *AggregateState, event TEvent) error {
	name := reflect.TypeOf(event).Name()

	handler, found := state.handlers[name]
	if !found {
		return fmt.Errorf("no handler registered for %s", name)
	}

	if err := handler(event); err != nil {
		return err
	}

	descriptor := EventDescriptor{
		AggregateID: state.id,
		Sequence:    state.sequence + len(state.pendingEvents) + 1,
		Timestamp:   time.Now().UTC(),
		EventType:   name,
		Event:       event,
	}

	state.pendingEvents = append(state.pendingEvents, descriptor)

	return nil
}

///

/// Save and Load

///

func SetID(state *AggregateState, id uuid.UUID) {
	state.id = id
}

func Sequence(state *AggregateState) int {
	return state.sequence
}

type EventDescriptor struct {
	AggregateID uuid.UUID
	Sequence    int
	Timestamp   time.Time
	EventType   string
	Event       any

	marshalled []byte
}

func (e *EventDescriptor) Marshal() ([]byte, error) {
	if len(e.marshalled) == 0 {
		content, err := json.Marshal(e.Event)
		if err != nil {
			return nil, err
		}

		e.marshalled = content
	}

	return e.marshalled, nil
}

func (a *AggregateState) ReplayEvent(event EventDescriptor) error {
	handler, found := a.handlers[event.EventType]
	if !found {
		return fmt.Errorf("no handler registered for %s", event.EventType)
	}

	if err := handler(event.Event); err != nil {
		return err
	}

	a.sequence = event.Sequence
	return nil
}
