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
