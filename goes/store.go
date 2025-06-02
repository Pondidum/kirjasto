package goes

import (
	"context"
	"kirjasto/tracing"
)

func Load(ctx context.Context, store *SqliteStore /* change to something more generic later*/, state *AggregateState) error {
	ctx, span := tr.Start(ctx, "load")
	defer span.End()

	for event, err := range store.Load(ctx, state.ID()) {
		if err != nil {
			return tracing.Error(span, err)
		}

		if err := state.ReplayEvent(event); err != nil {
			return err
		}
	}

	return nil
}

func Save(ctx context.Context, store *SqliteStore, state *AggregateState) error {

	pending := len(state.pendingEvents)
	if pending == 0 {
		return nil
	}

	if err := store.Save(ctx, state.ID(), Sequence(state), state.pendingEvents); err != nil {
		return err
	}

	state.sequence = state.pendingEvents[pending-1].Sequence
	state.pendingEvents = nil

	return nil
}
