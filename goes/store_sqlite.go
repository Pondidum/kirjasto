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
	"go.opentelemetry.io/otel/attribute"
)

var ErrNotFound = errors.New("aggregate does not exist")
var tr = otel.Tracer("goes")

type Store interface {
	Save(ctx context.Context, aggregate Aggregate) error
	Load(ctx context.Context, aggregateID uuid.UUID, aggregate Aggregate) error
}

type SqliteStore struct {
	db          *sql.DB
	factory     AggregateFactory
	projections []AggregateProjection
}

var _ Store = &SqliteStore{}

func NewSqliteStore(db *sql.DB) *SqliteStore {
	return &SqliteStore{
		db:          db,
		factory:     AggregateFactory{},
		projections: []AggregateProjection{},
	}
}

func (s *SqliteStore) RegisterAggregate(name string, factory func() Aggregate) {
	s.factory[name] = factory
}

func (s *SqliteStore) RegisterProjection(projection AggregateProjection) {
	s.projections = append(s.projections, projection)
}

func (s *SqliteStore) Initialise(ctx context.Context) error {
	ctx, span := tr.Start(ctx, "initialise_store")
	defer span.End()

	createTables := `
CREATE TABLE IF NOT EXISTS events(
	event_id integer primary key autoincrement,
	aggregate_id text not null,
	aggregate_type text not null,
	sequence integer not null,
	timestamp timestamp not null,
	event_type text not null,
	event_data text not null,
	constraint aggregate_sequence unique(aggregate_id, sequence) on conflict rollback
);

create table if not exists auto_projections(
	aggregate_id text primary key,
	view text not null
);

create table if not exists views(
	name text not null primary key,
	view text not null
);
`

	if _, err := s.db.ExecContext(ctx, createTables); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (s *SqliteStore) Save(ctx context.Context, aggregate Aggregate) error {
	ctx, span := tr.Start(ctx, "save")
	defer span.End()

	state := aggregate.state()
	pending := len(state.pendingEvents)
	if pending == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	if err := validateSequence(ctx, tx, state.id, state.sequence); err != nil {
		return tracing.Error(span, err)
	}

	if err := s.storeEvents(ctx, tx, state); err != nil {
		return err
	}

	if err := s.runProjections(ctx, tx, aggregate); err != nil {
		return tracing.Error(span, err)
	}

	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	state.sequence = state.pendingEvents[pending-1].Sequence
	state.pendingEvents = nil

	return nil
}

func (s *SqliteStore) storeEvents(ctx context.Context, tx *sql.Tx, state *AggregateState) error {
	insertEvent, err := tx.PrepareContext(ctx, `
		insert into events (aggregate_id, aggregate_type, sequence, timestamp, event_type, event_data)
		values (?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return err
	}

	for _, e := range state.pendingEvents {
		_, err = insertEvent.ExecContext(ctx, e.AggregateID, e.AggregateType, e.Sequence, e.Timestamp, e.EventType, e.json)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SqliteStore) runProjections(ctx context.Context, tx *sql.Tx, aggregate Aggregate) error {

	for _, ap := range s.projections {
		if err := ap.Project(ctx, tx, aggregate); err != nil {
			return err
		}
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

func (s *SqliteStore) Load(ctx context.Context, aggregateID uuid.UUID, aggregate Aggregate) error {
	ctx, span := tr.Start(ctx, "load")
	defer span.End()

	state := aggregate.state()
	SetID(state, aggregateID)

	count := 0
	for event, err := range s.eventsFor(ctx, state.id) {
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

func (s *SqliteStore) eventsFor(ctx context.Context, aggregateID uuid.UUID) iter.Seq2[EventDescriptor, error] {

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

			if err := rows.Scan(&e.Sequence, &e.Timestamp, &e.EventType, &e.json); err != nil {
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
			select aggregate_id, aggregate_type, sequence, timestamp, event_type, event_data
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

			if err := rows.Scan(&e.AggregateID, &e.AggregateType, &e.Sequence, &e.Timestamp, &e.EventType, &e.json); err != nil {
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

func (s *SqliteStore) RebuildAll(ctx context.Context) error {
	ctx, span := tr.Start(ctx, "rebuild")
	defer span.End()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	aggregates := map[uuid.UUID]Aggregate{}

	events := s.allEvents(ctx, tx)
	for event, err := range events {
		if err != nil {
			return tracing.Error(span, err)
		}

		// this should be a MRU cache really, and rather than assuming a non-existing entry means we are starting
		// from the first event, we should check the Sequence and load from the db if needed.
		aggregate, found := aggregates[event.AggregateID]
		if !found {
			factory, found := s.factory[event.AggregateType]
			if !found {
				return tracing.Errorf(span, "unable to find aggregate factory for '%s'", event.AggregateType)
			}

			aggregate = factory()
			aggregates[event.AggregateID] = aggregate
		}

		state := aggregate.state()
		if err := state.replayEvent(event); err != nil {
			return tracing.Error(span, err)
		}

		// done so that projections can read the events from state
		state.pendingEvents = append(state.pendingEvents, event)
	}

	for _, aggregate := range aggregates {

		if err := s.runProjections(ctx, tx, aggregate); err != nil {
			return tracing.Error(span, err)
		}

	}

	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
