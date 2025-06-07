package goes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

type viewDescriptor[TView any] struct {
	AggregateID uuid.UUID
	View        *TView
}

func NewSqlProjection[TView any]() *SqlProjection[TView] {
	name := reflect.TypeOf(*new(TView)).Name()

	return &SqlProjection[TView]{
		cache:    map[uuid.UUID]*viewDescriptor[TView]{},
		handlers: map[string]func(ctx context.Context, view *TView, event any) error{},

		createTable: fmt.Sprintf(`
			create table if not exists %s(
				aggregate_id text primary key,
				view blob not null
			);`, name),
		readView: fmt.Sprintf(`
			select view
			from %s
			where aggregate_id = @aggregate_id`, name),
		updateView: fmt.Sprintf(`
			insert into
				%s (aggregate_id, view)
				values (@aggregate_id, @view)
			on conflict(aggregate_id) do update set
				view = @view`, name),
		deleteAllViews: fmt.Sprintf(`
			delete
			from %s`, name),
	}
}

type SqlProjection[TView any] struct {
	Tx       *sql.Tx
	cache    map[uuid.UUID]*viewDescriptor[TView]
	handlers map[string]func(ctx context.Context, view *TView, event any) error

	createTable    string
	readView       string
	updateView     string
	deleteAllViews string
}

func (p *SqlProjection[TView]) addHandler(name string, handler func(ctx context.Context, view *TView, event any) error) {
	p.handlers[name] = handler
}

func AddProjectionHandler[TView any, TEvent any](p *SqlProjection[TView], projector func(ctx context.Context, view *TView, event TEvent) error) {
	name := reflect.TypeOf(*new(TEvent)).Name()

	p.addHandler(name, func(ctx context.Context, view *TView, event any) error {

		switch e := event.(type) {
		case TEvent:
			return projector(ctx, view, e)
		case *TEvent:
			return projector(ctx, view, *e)
		default:
			return fmt.Errorf("unable to handle %T", e)
		}
	})

	eventFactory[name] = func() any {
		return new(TEvent)
	}
}

func (p *SqlProjection[TView]) Load(ctx context.Context, tx *sql.Tx) error {
	p.Tx = tx
	clear(p.cache)

	// create the table
	if _, err := tx.ExecContext(ctx, p.createTable); err != nil {
		return err
	}
	return nil
}

func (p *SqlProjection[TView]) Project(ctx context.Context, event EventDescriptor) error {

	vd, found := p.cache[event.AggregateID]
	if !found {

		// load
		row := p.Tx.QueryRowContext(ctx,
			p.readView,
			sql.Named("aggregate_id", event.AggregateID.String()),
		)

		var viewJson []byte
		view := new(TView)

		if err := row.Scan(&viewJson); err != nil {
			if err != sql.ErrNoRows {
				return err
			}
		} else {
			if err := json.Unmarshal(viewJson, view); err != nil {
				return err
			}
		}

		vd = &viewDescriptor[TView]{
			AggregateID: event.AggregateID,
			View:        view,
		}
		p.cache[event.AggregateID] = vd
	}

	// figure out the handler
	name := event.EventType
	handler, found := p.handlers[name]
	if !found {
		return fmt.Errorf("no handler registered for %s", name)
	}
	if err := handler(ctx, vd.View, event.Event); err != nil {
		return err
	}

	return nil
}

func (p *SqlProjection[TView]) Save(ctx context.Context, tx *sql.Tx) error {

	for _, vd := range p.cache {

		json, err := json.Marshal(vd.View)
		if err != nil {
			return err
		}

		_, err = p.Tx.ExecContext(ctx, p.updateView,
			sql.Named("aggregate_id", vd.AggregateID.String()),
			sql.Named("view", json),
		)
		if err != nil {
			return err
		}

	}

	clear(p.cache)
	p.Tx = nil
	return nil
}

func (p *SqlProjection[TView]) Wipe(ctx context.Context) error {

	_, err := p.Tx.ExecContext(ctx, p.deleteAllViews)
	if err != nil {
		return err
	}

	return nil
}

func (p *SqlProjection[TView]) View(ctx context.Context, reader Readable, aggregateID uuid.UUID) (*TView, error) {

	row := reader.QueryRowContext(ctx,
		p.readView,
		sql.Named("aggregate_id", aggregateID.String()),
	)

	var viewJson []byte
	view := new(TView)

	if err := row.Scan(&viewJson); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(viewJson, view); err != nil {
		return nil, err
	}

	return view, nil
}

type Readable interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
