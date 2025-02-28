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

	engine := ui.NewTemplateEngine()
	if err := engine.ParseTemplates(ctx); err != nil {
		return tracing.Error(span, err)
	}

	server := http.NewServeMux()
	server.HandleFunc("GET /catalogue", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/catalogue.html", w); err != nil {
			w.WriteHeader(500)
		}
	})

	fmt.Println("Listening on", c.address)
	if err := http.ListenAndServe(c.address, server); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
