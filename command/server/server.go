package server

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/tracing"
	"kirjasto/ui"
	"net/http"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var tr = otel.Tracer("command.server")

func NewServerCommand() *ServerCommand {
	return &ServerCommand{}
}

type ServerCommand struct {
	address string
}

func (c *ServerCommand) Synopsis() string {
	return "runs the server"
}

func (c *ServerCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("server", pflag.ContinueOnError)
	flags.StringVar(&c.address, "address", "localhost:4400", "host:port")
	return flags
}

func (c *ServerCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	mux := http.NewServeMux()

	if err := ui.RegisterUI(ctx, config, mux); err != nil {
		return tracing.Error(span, err)
	}

	server := &http.Server{
		Addr:    c.address,
		Handler: otelhttp.NewHandler(mux, "mux"),
	}

	fmt.Println("Listening on", server.Addr)
	go func() {
		server.ListenAndServe()
	}()
	<-ctx.Done()

	if err := server.Shutdown(context.Background()); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
