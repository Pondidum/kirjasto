package landing

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/routing"
	"kirjasto/storage"
	"kirjasto/template"
	"kirjasto/tracing"
	"net/http"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("ui.landing")

func RegisterHandlers(ctx context.Context, config *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {
	mux.HandleFunc("GET /", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {
		ctx, span := tr.Start(r.Context(), "get_landing")
		defer span.End()

		if err := r.ParseForm(); err != nil {
			return tracing.Error(span, err)
		}

		filter := &FilterOptions{
			Filter:    r.FormValue("filter"),
			Type:      r.FormValue("type"),
			Ownership: r.FormValue("ownership"),
			Progress:  r.FormValue("progress"),
		}

		reader, err := storage.Reader(ctx, config.DatabaseFile)
		if err != nil {
			return tracing.Error(span, err)
		}

		p := domain.NewLibraryProjection()
		library, err := p.View(ctx, reader, domain.LibraryID)
		if err != nil {
			return tracing.Error(span, err)
		}

		dto := map[string]any{
			"Filter":  filter,
			"Library": library,
		}

		fmt.Println(filter.Filter, filter.Ownership, filter.Progress, filter.Type)

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "landing/landing.html", dto, w); err != nil {
			return tracing.Error(span, err)
		}
		return nil
	}))
	return nil
}

type FilterOptions struct {
	Filter    string
	Type      string
	Ownership string
	Progress  string
}
