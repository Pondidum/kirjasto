package goes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"kirjasto/tracing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel"
)

var ErrNotFound = errors.New("aggregate does not exist")
var tr = otel.Tracer("goes")

func NewSqliteStore(db *sql.DB) *SqliteStore {
	return &SqliteStore{
		db: db,
	}
}

type SqliteStore struct {
	db *sql.DB
}

func (s *SqliteStore) Initialise(ctx context.Context) error {
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

	if _, err := s.db.ExecContext(ctx, createTables); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (s *SqliteStore) Save(ctx context.Context, aggregateID uuid.UUID, sequence int, events []EventDescriptor) error {
	ctx, span := tr.Start(ctx, "save")
	defer span.End()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	if err := validateSequence(ctx, tx, aggregateID, sequence); err != nil {
		return tracing.Error(span, err)
	}

	if err := projectionist.Load(ctx, tx); err != nil {
		return tracing.Error(span, err)
	}

	writer, err := newEventWriter(ctx, tx)
	if err != nil {
		return tracing.Error(span, err)
	}

	for _, event := range events {
		if err := writer.Write(ctx, event); err != nil {
			return err
		}
		if err := projectionist.Project(ctx, event); err != nil {
			return tracing.Error(span, err)
		}
	}

	if err := projectionist.Save(ctx, tx); err != nil {
		return tracing.Error(span, err)
	}

	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func validateSequence(ctx context.Context, tx *sql.Tx, aggregateID uuid.UUID, memorySequence int) error {

	var dbSequence sql.NullInt64
	if err := tx.QueryRowContext(ctx, "select max(sequence) from events where aggregate_id = ?", aggregateID).Scan(&dbSequence); err != nil {
		if err != sql.ErrNoRows {
			return err
		}
	}

	if dbSequence.Valid && dbSequence.Int64 > int64(memorySequence) {
		return fmt.Errorf("aggregate has new events in the database. db: %v, memory: %v", dbSequence, memorySequence)
	}

	return nil
}

func (s *SqliteStore) Load(ctx context.Context, aggregateID uuid.UUID) iter.Seq2[EventDescriptor, error] {
	return func(yield func(EventDescriptor, error) bool) {

		rows, err := s.db.QueryContext(ctx, `
			select sequence, timestamp, event_type, event_data
			from events
			where aggregate_id = @aggregate_id
			order by sequence asc
		`, sql.Named("aggregate_id", aggregateID.String()))
		if err != nil {
			if !yield(EventDescriptor{}, err) {
				return
			}
		}
		defer rows.Close()

		for rows.Next() {

			e := EventDescriptor{
				AggregateID: aggregateID,
			}

			var eventJson []byte

			if err := rows.Scan(&e.Sequence, &e.Timestamp, &e.EventType, &eventJson); err != nil {
				if !yield(e, err) {
					return
				}
			}

			if e.Event, err = eventFromJson(e.EventType, eventJson); err != nil {
				if !yield(e, err) {
					return
				}
			}

			if !yield(e, nil) {
				return
			}
		}

	}
}

func (s *SqliteStore) allEvents(ctx context.Context, tx *sql.Tx) iter.Seq2[EventDescriptor, error] {
	return func(yield func(EventDescriptor, error) bool) {

		rows, err := tx.QueryContext(ctx, `
			select aggregate_id, sequence, timestamp, event_type, event_data
			from events
			order by event_id asc
		`)
		if err != nil {
			if !yield(EventDescriptor{}, err) {
				return
			}
		}
		defer rows.Close()

		for rows.Next() {

			e := EventDescriptor{}

			var eventJson []byte
			if err := rows.Scan(&e.AggregateID, &e.Sequence, &e.Timestamp, &e.EventType, &eventJson); err != nil {
				if !yield(e, err) {
					return
				}
			}

			if e.Event, err = eventFromJson(e.EventType, eventJson); err != nil {
				if !yield(e, err) {
					return
				}
			}

			if !yield(e, nil) {
				return
			}
		}
	}
}

func (s *SqliteStore) Rebuild(ctx context.Context, projection Projection) error {
	ctx, span := tr.Start(ctx, "rebuild")
	defer span.End()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	if err := projection.Load(ctx, tx); err != nil {
		return tracing.Error(span, err)
	}

	if err := projection.Wipe(ctx); err != nil {
		return tracing.Error(span, err)
	}

	for event, err := range s.allEvents(ctx, tx) {
		if err != nil {
			return tracing.Error(span, err)
		}

		if err := projection.Project(ctx, event); err != nil {
			return tracing.Error(span, err)
		}
	}

	if err := projection.Save(ctx, tx); err != nil {
		return tracing.Error(span, err)
	}

	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
