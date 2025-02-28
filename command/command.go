package command

import (
	"context"
	"fmt"
	"kirjasto/command/version"
	"kirjasto/config"
	"kirjasto/tracing"
	"os"
	"strings"

	"github.com/hashicorp/cli"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
)

type CommandDefinition interface {
	Synopsis() string
	Flags() *pflag.FlagSet
	Execute(ctx context.Context, cfg *config.Config, args []string) error
}

func NewCommand(definition CommandDefinition) func() (cli.Command, error) {
	return func() (cli.Command, error) {
		return &command{definition}, nil
	}
}

type command struct {
	CommandDefinition
}

func (c *command) Help() string {
	sb := strings.Builder{}

	sb.WriteString(c.Synopsis())
	sb.WriteString("\n\n")

	sb.WriteString("Flags:\n\n")

	sb.WriteString(c.Flags().FlagUsagesWrapped(80))

	return sb.String()
}

func (c *command) Run(args []string) int {
	ctx := context.Background()

	cfg, err := config.CreateConfig(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	shutdown, err := tracing.Configure(ctx, "kirjasto", version.VersionNumber())
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	defer shutdown(ctx)

	tr := otel.Tracer("kirjasto")
	ctx, span := tr.Start(ctx, "main")
	defer span.End()

	flags := c.Flags()

	if err := flags.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	if err := c.Execute(ctx, cfg, flags.Args()); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	return 0
}
