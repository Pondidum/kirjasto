package goes

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type Aggregate interface {
	state() *AggregateState
}

func NewAggregateState() *AggregateState {
	return &AggregateState{
		sequence: -1,
		handlers: map[string]func(event any) error{},
	}
}

type AggregateState struct {
	id       uuid.UUID
	sequence int

	handlers map[string]func(event any) error

	pendingEvents []EventDescriptor
}

func (as *AggregateState) state() *AggregateState {
	return as
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

func nameOf(aggregate Aggregate) string {
	return reflect.TypeOf(aggregate).Elem().Name()
}

func Apply[TEvent any](aggregate Aggregate, event TEvent) error {
	eventName := reflect.TypeOf(event).Name()
	aggregateName := nameOf(aggregate)

	state := aggregate.state()
	handler, found := state.handlers[eventName]
	if !found {
		return fmt.Errorf("no handler registered for %s", eventName)
	}

	if err := handler(event); err != nil {
		return err
	}

	content, err := json.Marshal(event)
	if err != nil {
		return err
	}

	descriptor := EventDescriptor{
		AggregateID:   state.id,
		AggregateType: aggregateName,
		Sequence:      state.sequence + len(state.pendingEvents) + 1,
		Timestamp:     time.Now().UTC(),
		EventType:     eventName,
		json:          content,
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

func (a *AggregateState) replayEvent(ed EventDescriptor) error {
	event, err := ed.Event()
	if err != nil {
		return err
	}

	handler, found := a.handlers[ed.EventType]
	if !found {
		return fmt.Errorf("no handler registered for %s", ed.EventType)
	}

	if err := handler(event); err != nil {
		return err
	}

	a.sequence = ed.Sequence
	return nil
}
