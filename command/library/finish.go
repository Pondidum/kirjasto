package library

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/tracing"
	"time"

	"github.com/spf13/pflag"
)

func NewFinishCommand() *FinishCommand {
	return &FinishCommand{}
}

type FinishCommand struct {
	when string
}

func (c *FinishCommand) Name() string {
	return "library finish"
}

func (c *FinishCommand) Synopsis() string {
	return "mark a book at in-progress"
}

func (c *FinishCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("finish", pflag.ContinueOnError)
	flags.StringVar(&c.when, "when", "", "when the book was finished in yyyy-mm-dd format")
	return flags
}

func (c *FinishCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command takes exactly 1 argument: isbn")
	}

	isbn := args[0]
	when := time.Now()
	if c.when != "" {
		t, err := time.Parse("2006-01-02", c.when)
		if err != nil {
			return tracing.Error(span, err)
		}
		when = t
	}

	err := libraryAction(ctx, config, func(library *domain.Library) error {
		return library.FinishReading(isbn, when)
	})
	if err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
