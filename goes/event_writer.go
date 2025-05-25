package goes

import (
	"context"
	"database/sql"
)

type eventWriter struct {
	*sql.Stmt
}

func newEventWriter(ctx context.Context, tx *sql.Tx) (*eventWriter, error) {

	insertEvent, err := tx.PrepareContext(ctx, `
insert into
	events (
			aggregate_id,
			sequence,
			timestamp,
			event_type,
			event_data
	)
values (?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, err
	}

	return &eventWriter{Stmt: insertEvent}, nil
}

func (ew *eventWriter) Write(ctx context.Context, e EventDescriptor) error {

	eventJson, err := e.Marshal()
	if err != nil {
		return err
	}

	if _, err := ew.ExecContext(ctx, e.AggregateID, e.Sequence, e.Timestamp, e.EventType, eventJson); err != nil {
		return err
	}
	return nil
}
