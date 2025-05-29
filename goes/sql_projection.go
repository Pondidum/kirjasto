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
	Sequence int64
	View     *TView
}

func NewSqlProjection[TView any]() *SqlProjection[TView] {
	return &SqlProjection[TView]{
		name:     reflect.TypeOf(*new(TView)).Name(),
		cache:    map[uuid.UUID]*viewDescriptor[TView]{},
		handlers: map[string]func(ctx context.Context, view *TView, event any) error{},
	}
}

type SqlProjection[TView any] struct {
	name     string
	tx       *sql.Tx
	cache    map[uuid.UUID]*viewDescriptor[TView]
	handlers map[string]func(ctx context.Context, view *TView, event any) error
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
	p.tx = tx
	clear(p.cache)

	// create the table
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`
create table if not exists %s(
	aggregate_id text primary key,
	sequence integer not null,
	view blob not null
);`, p.name))
	if err != nil {
		return err
	}
	return nil
}

func (p *SqlProjection[TView]) Project(ctx context.Context, event EventDescriptor) error {

	vd, found := p.cache[event.AggregateID]
	if !found {

		// load
		row := p.tx.QueryRowContext(ctx,
			fmt.Sprintf(`select sequence, view from %s where aggregate_id = @aggregate_id`, p.name),
			sql.Named("aggregate_id", event.AggregateID.String()),
		)

		var sequence int64
		var viewJson []byte
		var view *TView

		if err := row.Scan(&sequence, &viewJson); err != nil {
			if err != sql.ErrNoRows {
				return err
			}
			view = new(TView)
		} else {
			if err := json.Unmarshal(viewJson, view); err != nil {
				return err
			}
		}

		vd = &viewDescriptor[TView]{
			Sequence: sequence,
			View:     view,
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

	update := fmt.Sprintf(`
	insert into
		%s (aggregate_id, sequence, view)
		values (@aggregate_id, @sequence, @view)
	on conflict(aggregate_id) do update set
		sequence = @sequence,
		view = @view
`, p.name)

	// write to table
	json, err := json.Marshal(vd.View)
	if err != nil {
		return err
	}

	_, err = p.tx.ExecContext(ctx, update,
		sql.Named("aggregate_id", event.AggregateID.String()),
		sql.Named("sequence", event.Sequence),
		sql.Named("view", json),
	)
	return err

}

func (p *SqlProjection[TView]) Save(ctx context.Context, tx *sql.Tx) error {
	clear(p.cache)
	p.tx = nil
	return nil
}
