package goes

import (
	"context"
	"iter"
	"kirjasto/tracing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
)

type Store interface {
	Save(ctx context.Context, aggregateID uuid.UUID, sequence int, events []EventDescriptor) error
	Load(ctx context.Context, aggregateID uuid.UUID) iter.Seq2[EventDescriptor, error]
}

func Load(ctx context.Context, store Store, state *AggregateState) error {
	ctx, span := tr.Start(ctx, "load")
	defer span.End()

	count := 0
	for event, err := range store.Load(ctx, state.id) {
		if err != nil {
			return tracing.Error(span, err)
		}

		count++

		if err := state.replayEvent(event); err != nil {
			return err
		}
	}

	span.SetAttributes(attribute.Int("event.count", count))
	if count == 0 {
		return ErrNotFound
	}

	return nil
}

type Aggregate interface {
	state() *AggregateState
}

func Save(ctx context.Context, store Store, aggregate Aggregate) error {
	state := aggregate.state()
	pending := len(state.pendingEvents)
	if pending == 0 {
		return nil
	}

	if err := store.Save(ctx, state.id, Sequence(state), state.pendingEvents); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pending-1].Sequence
	state.pendingEvents = nil

	return nil
}
