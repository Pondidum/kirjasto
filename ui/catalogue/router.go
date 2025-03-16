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
		dto := map[string]any{
			"Results": []string{},
		}

		if err := r.ParseForm(); err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(400)
		}

		if query := r.Form.Get("query"); query != "" {
			reader, err := storage.Reader(ctx, config.DatabaseFile)
			if err != nil {
				w.Write([]byte(err.Error()))
				w.WriteHeader(500)
				return
			}
			results, err := QueryBooks(ctx, reader, 1000, query)
			if err != nil {
				w.Write([]byte(err.Error()))
				w.WriteHeader(500)
				return
			}

			dto["Results"] = results
		}

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/catalogue.html", dto, w); err != nil {
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
