package goes

import (
	"context"
	"database/sql"
	"encoding/json"
	"kirjasto/tracing"
)

type AggregateProjection interface {
	Project(ctx context.Context, tx *sql.Tx, aggregate Aggregate) error
}

type AutoProjection struct{}

func (ap *AutoProjection) Project(ctx context.Context, tx *sql.Tx, aggregate Aggregate) error {
	ctx, span := tr.Start(ctx, "autoprojection")
	defer span.End()

	view, err := json.Marshal(aggregate)
	if err != nil {
		return tracing.Error(span, err)
	}
	stmt := `
		insert into auto_projections (aggregate_id, view)
		values (@aggregate_id, @view)
		on conflict(aggregate_id) do update set
			view = excluded.view
	`
	_, err = tx.ExecContext(ctx,
		stmt,
		sql.Named("aggregate_id", aggregate.state().id.String()),
		sql.Named("view", string(view)))
	if err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

type Projector[TView any] = func(ctx context.Context, view *TView, event EventDescriptor) error

func NewEventProjection[TView any](name string, newView func() *TView, project Projector[TView]) *EventProjection[TView] {
	return &EventProjection[TView]{
		name:    name,
		newView: newView,
		project: project,
	}
}

type EventProjection[TView any] struct {
	name    string
	newView func() *TView
	project func(ctx context.Context, view *TView, event EventDescriptor) error
}

func (ep *EventProjection[TView]) Project(ctx context.Context, tx *sql.Tx, aggregate Aggregate) error {

	view, err := ep.loadView(ctx, tx)
	if err != nil {
		return err
	}

	events := aggregate.state().pendingEvents
	for _, event := range events {
		if err := ep.project(ctx, view, event); err != nil {
			return err
		}
	}

	if err := ep.saveView(ctx, tx, view); err != nil {
		return err
	}

	return nil
}

func (ep *EventProjection[TView]) loadView(ctx context.Context, tx *sql.Tx) (*TView, error) {

	stmt := `select view from views where name = @name`
	row := tx.QueryRowContext(ctx, stmt, sql.Named("name", ep.name))
	if err := row.Err(); err != nil {
		return nil, err
	}

	view := ep.newView()
	var viewJson []byte
	err := row.Scan(&viewJson)
	if err == sql.ErrNoRows {
		return view, nil
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(viewJson, &view); err != nil {
		return nil, err
	}

	return view, nil
}

func (ep *EventProjection[TView]) saveView(ctx context.Context, tx *sql.Tx, view *TView) error {
	stmt := `
		insert into views(name, view) values (@name, @view)
		on conflict(name) do update set view = excluded.view
	`

	viewJson, err := json.Marshal(view)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		stmt,
		sql.Named("name", ep.name),
		sql.Named("view", viewJson))

	return err
}
