package goes

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
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

func (c *GoesProjectionCommand) Name() string {
	return "goes rebuild views"
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

	db, err := storage.Writer(ctx, config.LibraryDb)
	if err != nil {
		return tracing.Error(span, err)
	}

	eventStore, err := domain.NewStore(ctx, db)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := eventStore.RebuildAll(ctx); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
