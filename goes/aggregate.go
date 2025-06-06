package goes

import (
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

	eventFactory   map[string]func() any
	autoProjection func() any
}

func NewAggregateState() *AggregateState {
	return &AggregateState{
		sequence:     -1,
		handlers:     map[string]func(event any) error{},
		eventFactory: map[string]func() any{},
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

	state.eventFactory[name] = func() any {
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

func RegisterAutoProjection[TView any](state *AggregateState, project func() TView) {
	state.autoProjection = func() any {
		return project()
	}
}

func newEvent(state *AggregateState, eventType string) (any, error) {
	if factory, found := state.eventFactory[eventType]; found {
		return factory(), nil
	}

	return nil, fmt.Errorf("no factory for %s found", eventType)
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
}

func SaveEvents(state *AggregateState, write func(e EventDescriptor) error) error {

	for _, event := range state.pendingEvents {
		if err := write(event); err != nil {
			return err
		}

		state.sequence = event.Sequence
	}

	state.pendingEvents = nil
	return nil
}

func LoadEvents(state *AggregateState, events []EventDescriptor) error {
	for _, e := range events {

		handler, found := state.handlers[e.EventType]
		if !found {
			return fmt.Errorf("no handler registered for %s", e.EventType)
		}

		if err := handler(e.Event); err != nil {
			return err
		}

		state.sequence = e.Sequence
	}

	return nil
}
