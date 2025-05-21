package goes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"kirjasto/tracing"
	"reflect"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel"
)

var ErrNotFound = errors.New("aggregate does not exist")
var tr = otel.Tracer("goes")

func InitialiseStore(ctx context.Context, db *sql.DB) error {
	ctx, span := tr.Start(ctx, "initialise_store")
	defer span.End()

	createTables := `
CREATE TABLE IF NOT EXISTS events(
	event_id integer primary key autoincrement,
	aggregate_id text not null,
	sequence integer not null,
	timestamp timestamp not null,
	event_type text not null,
	event_data text not null,
	constraint aggregate_sequence unique(aggregate_id, sequence) on conflict rollback
);

create table if not exists auto_projections(
	aggregate_id text primary key,
	view_type text not null,
	view text not null
);`

	if _, err := db.ExecContext(ctx, createTables); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func Save(ctx context.Context, db *sql.DB, state *AggregateState) error {
	ctx, span := tr.Start(ctx, "save")
	defer span.End()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	var dbSequence sql.NullInt64
	if err := tx.QueryRowContext(ctx, "select max(sequence) from events where aggregate_id = ?", state.ID()).Scan(&dbSequence); err != nil {
		if err != sql.ErrNoRows {
			return tracing.Error(span, err)
		}
	}

	sequence := Sequence(state)

	if dbSequence.Valid && dbSequence.Int64 > int64(sequence) {
		return fmt.Errorf("aggregate has new events in the database. db: %v, memory: %v", dbSequence, sequence)
	}

	insertEvent, err := tx.PrepareContext(ctx, `insert into events (aggregate_id, sequence, timestamp, event_type, event_data) values (?, ?, ?, ?, ?)`)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer insertEvent.Close()

	err = SaveEvents(state, func(e EventDescriptor) error {

		eventJson, err := json.Marshal(e.Event)
		if err != nil {
			return tracing.Error(span, err)
		}

		if _, err := insertEvent.ExecContext(ctx, e.AggregateID, e.Sequence, e.Timestamp, e.EventType, eventJson); err != nil {
			return tracing.Error(span, err)
		}

		return nil
	})
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := writeAutoProjection(ctx, tx, state); err != nil {
		return tracing.Error(span, err)
	}

	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func writeAutoProjection(ctx context.Context, tx *sql.Tx, state *AggregateState) error {
	ctx, span := tr.Start(ctx, "write_auto_projections")
	defer span.End()

	if state.autoProjection == nil {
		return nil
	}

	view := state.autoProjection()
	if view == nil {
		return nil
	}

	viewType := reflect.TypeOf(view).Name()
	viewJson, err := json.Marshal(view)
	if err != nil {
		return tracing.Error(span, err)
	}

	updateView := `
		insert into auto_projections (aggregate_id, view_type, view)
		values (?, ?, ?)
		on conflict(aggregate_id)
		do update set view=excluded.view`

	if _, err := tx.ExecContext(ctx, updateView, state.ID(), viewType, viewJson); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func Load(ctx context.Context, db *sql.DB, state *AggregateState, id uuid.UUID) error {
	ctx, span := tr.Start(ctx, "load")
	defer span.End()

	rows, err := db.QueryContext(ctx, "select sequence, timestamp, event_type, event_data from events where aggregate_id = ?", id)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer rows.Close()

	events := []EventDescriptor{}
	for rows.Next() {

		e := EventDescriptor{
			AggregateID: state.ID(),
		}

		var eventJson []byte

		if err := rows.Scan(&e.Sequence, &e.Timestamp, &e.EventType, &eventJson); err != nil {
			return tracing.Error(span, err)
		}

		if e.Event, err = newEvent(state, e.EventType); err != nil {
			return tracing.Error(span, err)
		}

		if err := json.Unmarshal(eventJson, &e.Event); err != nil {
			return tracing.Error(span, err)
		}

		events = append(events, e)
	}

	if len(events) == 0 {
		return tracing.Error(span, ErrNotFound)
	}

	return LoadEvents(state, events)
}

func ViewById(ctx context.Context, db *sql.DB, aggregateID uuid.UUID, view any) error {
	ctx, span := tr.Start(ctx, "view_by_id")
	defer span.End()

	query := `select view from auto_projections where aggregate_id = ?`
	viewJson := ""

	if err := db.QueryRowContext(ctx, query, aggregateID).Scan(&viewJson); err != nil {
		return tracing.Error(span, err)
	}

	if err := json.Unmarshal([]byte(viewJson), &view); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func ViewByProperty(ctx context.Context, db *sql.DB, path string, value any, view any) error {
	ctx, span := tr.Start(ctx, "view_by_property")
	defer span.End()

	viewType := reflect.TypeOf(view).Name()
	if viewType == "" {
		viewType = reflect.TypeOf(view).Elem().Name()
	}

	query := `select view from auto_projections where view_type = ? and view ->> ? = ?`
	viewJson := ""

	if err := db.QueryRowContext(ctx, query, viewType, path, value).Scan(&viewJson); err != nil {
		return tracing.Error(span, err)
	}

	if err := json.Unmarshal([]byte(viewJson), &view); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
