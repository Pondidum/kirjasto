package catalogue

import (
	"context"
	"kirjasto/config"
	"kirjasto/routing"
	"kirjasto/storage"
	"kirjasto/template"
	"net/http"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("ui.catalogue")

func RegisterHandlers(ctx context.Context, config *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {

	mux.HandleFunc("GET /catalogue", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {
		ctx, span := tr.Start(r.Context(), "get catalogue")
		defer span.End()

		dto := map[string]any{
			"Results": []string{},
		}

		if err := r.ParseForm(); err != nil {
			return err
		}

		if query := r.Form.Get("query"); query != "" {
			books, err := storage.FindBookByTitle(ctx, query)
			if err != nil {
				return err
			}

			dto["Results"] = books
		}

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/catalogue.html", dto, w); err != nil {
			return err
		}

		return nil
	}))

	mux.HandleFunc("GET /catalogue/book/{id}", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {

		id := r.PathValue("id")

		dto := &storage.Book{
			ID: id,
		}

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/book.html", dto, w); err != nil {
			return err
		}

		return nil
	}))

	return nil
}
