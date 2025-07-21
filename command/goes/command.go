package goes

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/goes"
	"kirjasto/storage"
	"kirjasto/tracing"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("command.goes")

func NewGoesCommand() *GoesProjectionCommand {
	return &GoesProjectionCommand{}
}

type GoesProjectionCommand struct {
}

func (c *GoesProjectionCommand) Synopsis() string {
	return "rerun the projections"
}

func (c *GoesProjectionCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("server", pflag.ContinueOnError)
	return flags
}

func (c *GoesProjectionCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	db, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	eventStore := goes.NewSqliteStore(db)
	if err := eventStore.Initialise(ctx); err != nil {
		return tracing.Error(span, err)
	}

	projection := domain.NewLibraryProjection()
	if err := eventStore.RegisterProjection("library_view", projection); err != nil {
		return tracing.Error(span, err)
	}

	if err := eventStore.Rebuild(ctx, projection.SqlProjection); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
