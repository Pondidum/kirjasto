package library

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/tracing"
	"time"

	"github.com/spf13/pflag"
)

func NewStartCommand() *StartCommand {
	return &StartCommand{}
}

type StartCommand struct {
	when string
}

func (c *StartCommand) Synopsis() string {
	return "mark a book at in-progress"
}

func (c *StartCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("start", pflag.ContinueOnError)
	flags.StringVar(&c.when, "when", "", "when the book was started in yyyy-mm-dd format")
	return flags
}

func (c *StartCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
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
		return library.StartReading(isbn, when)
	})
	if err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
