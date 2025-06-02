package goes

import (
	"context"
	"database/sql"
)

// assuming all projections happen inside a sql tx for ease
type Projection interface {
	Load(ctx context.Context, tx *sql.Tx) error
	Project(ctx context.Context, event EventDescriptor) error
	Save(ctx context.Context, tx *sql.Tx) error
	Wipe(ctx context.Context) error
}

func StatelessProjection(action func(ctx context.Context, event EventDescriptor) error) Projection {
	return &statelessProjection{
		action: action,
	}
}

type statelessProjection struct {
	action func(ctx context.Context, event EventDescriptor) error
}

func (p *statelessProjection) Load(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func (p *statelessProjection) Project(ctx context.Context, event EventDescriptor) error {
	return p.action(ctx, event)
}

func (p *statelessProjection) Save(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func (p *statelessProjection) Wipe(ctx context.Context) error {
	return nil
}
