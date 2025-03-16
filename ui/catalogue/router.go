package catalogue

import (
	"context"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/template"
	"net/http"
)

func RegisterHandlers(ctx context.Context, config *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {

	mux.HandleFunc("GET /catalogue", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/catalogue.html", nil, w); err != nil {
			w.WriteHeader(500)
		}
	})

	mux.HandleFunc("GET /catalogue/book/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		dto := &Book{
			BookId: id,
		}

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/book.html", dto, w); err != nil {
			w.WriteHeader(500)
		}
	})

	return nil
}

type Book struct {
	BookId string
}
