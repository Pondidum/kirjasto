package library

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

var tr = otel.Tracer("command.library")

func NewAddCommand() *AddCommand {
	return &AddCommand{}
}

type AddCommand struct {
	tags []string
}

func (c *AddCommand) Synopsis() string {
	return "add a book to the library"
}

func (c *AddCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("add", pflag.ContinueOnError)
	flags.StringSliceVar(&c.tags, "tags", []string{}, "tags to add to the book")
	return flags
}

func (c *AddCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command expects 1 argument: isbn")
	}

	isbn := args[0]

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	store := goes.NewSqliteStore(writer)
	library, err := domain.LoadLibrary(ctx, store, domain.LibraryID)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := library.AddBook([]string{isbn}, c.tags); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
