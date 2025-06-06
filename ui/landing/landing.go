package landing

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/routing"
	"kirjasto/template"
	"net/http"
)

func RegisterHandlers(ctx context.Context, config *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {
	mux.HandleFunc("GET /", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {

		if err := r.ParseForm(); err != nil {
			return err
		}

		filter := &FilterOptions{
			Filter:    r.FormValue("filter"),
			Type:      r.FormValue("type"),
			Ownership: r.FormValue("ownership"),
			Progress:  r.FormValue("progress"),
		}

		// reader, err := storage.Reader(ctx, config.DatabaseFile)
		// if err != nil {
		// 	return err
		// }

		// library, err := storage.LibraryContents(ctx, reader)
		// if err != nil {
		// 	return err
		// }

		dto := map[string]any{
			"Filter": filter,
		}

		fmt.Println(filter.Filter, filter.Ownership, filter.Progress, filter.Type)

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "landing/landing.html", dto, w); err != nil {
			return err
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
