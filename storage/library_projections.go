package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"kirjasto/goes"

	"github.com/google/uuid"
)

var insert = `
insert into
	library_projections (aggregate_id, sequence, view)
	values (@aggregate_id, @sequence, @view)
`

var update = `
update library_projections set
	sequence = @sequence,
	view = @view
where
	aggregate_id = @aggregate_id
`

type viewDescriptor[TView any] struct {
	Sequence int64
	View     TView
}

type LibraryView struct {
	Shelves map[string]struct{}
}

func NewLibraryProjection() goes.Projection {
	return &LibraryProjection{
		cache: map[uuid.UUID]*viewDescriptor[LibraryView]{},
	}
}

type LibraryProjection struct {
	tx    *sql.Tx
	cache map[uuid.UUID]*viewDescriptor[LibraryView]
}

func (p *LibraryProjection) Load(ctx context.Context, tx *sql.Tx) error {
	p.tx = tx
	clear(p.cache)

	// create the table
	_, err := tx.ExecContext(ctx, `
create table if not exists library_projections(
	aggregate_id text primary key,
	sequence integer not null,
	view blob not null
);
	`)
	if err != nil {
		return err
	}
	return nil
}

func (p *LibraryProjection) Project(ctx context.Context, event goes.EventDescriptor) error {

	switch e := event.Event.(type) {
	case LibraryCreated:

		json, err := json.Marshal(LibraryView{
			Shelves: map[string]struct{}{},
		})
		if err != nil {
			return err
		}

		_, err = p.tx.ExecContext(ctx, insert,
			sql.Named("aggregate_id", event.AggregateID.String()),
			sql.Named("sequence", event.Sequence),
			sql.Named("view", json),
		)
		return err

	case BookImported:
		vd, found := p.cache[event.AggregateID]
		if !found {

			// load
			row := p.tx.QueryRowContext(ctx, `select sequence, view from library_projections where aggregate_id = @aggregate_id`,
				sql.Named("aggregate_id", event.AggregateID.String()),
			)

			var sequence int64
			var viewJson []byte
			if err := row.Scan(&sequence, &viewJson); err != nil {
				return err
			}

			var view LibraryView
			if err := json.Unmarshal(viewJson, &view); err != nil {
				return err
			}

			vd = &viewDescriptor[LibraryView]{
				Sequence: sequence,
				View:     view,
			}
			p.cache[event.AggregateID] = vd
		}

		// do things with view
		for _, shelf := range e.Shelves {
			vd.View.Shelves[shelf] = struct{}{}
		}

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

	return nil
}

func (p *LibraryProjection) Save(ctx context.Context, tx *sql.Tx) error {
	clear(p.cache)
	p.tx = nil
	return nil
}
