package goes

import (
	"context"
	"database/sql"
	"fmt"
	"kirjasto/tracing"
)

type Projectionist struct {
	projections map[string]Projection
}

func (p *Projectionist) RegisterProjection(name string, projection Projection) error {
	if _, found := p.projections[name]; found {
		return fmt.Errorf("a projection with the name '%s' already exists", name)
	}

	p.projections[name] = projection
	return nil
}
func (p *Projectionist) Load(ctx context.Context, tx *sql.Tx) error {
	ctx, span := tr.Start(ctx, "load")
	defer span.End()

	for _, projection := range p.projections {
		if err := projection.Load(ctx, tx); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}

func (p *Projectionist) Project(ctx context.Context, event EventDescriptor) error {
	ctx, span := tr.Start(ctx, "project")
	defer span.End()

	for _, projection := range p.projections {
		if err := projection.Project(ctx, event); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}

func (p *Projectionist) Save(ctx context.Context, tx *sql.Tx) error {
	ctx, span := tr.Start(ctx, "save")
	defer span.End()

	for _, projection := range p.projections {
		if err := projection.Save(ctx, tx); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}
