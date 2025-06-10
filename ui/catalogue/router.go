package catalogue

import (
	"context"
	"kirjasto/config"
	"kirjasto/openlibrary"
	"kirjasto/routing"
	"kirjasto/storage"
	"kirjasto/template"
	"net/http"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("ui.catalogue")

func RegisterHandlers(ctx context.Context, config *config.Config, mux *http.ServeMux, engine *template.TemplateEngine) error {

	mux.HandleFunc("GET /catalogue", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {
		ctx, span := tr.Start(r.Context(), "get_catalogue")
		defer span.End()

		dto := map[string]any{
			"Results": []string{},
		}

		form, err := routing.Form(r)
		if err != nil {
			return err
		}
		dto["QueryParams"] = form

		reader, err := storage.Reader(ctx, config.DatabaseFile)
		if err != nil {
			return err
		}

		if query, found := form["query"]; found {

			books, err := openlibrary.FindBooks(ctx, reader, query)
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

	mux.HandleFunc("GET /catalogue/books/{id}/{isbn}", routing.RouteHandler(func(w http.ResponseWriter, r *http.Request) error {
		_, span := tr.Start(r.Context(), "get_edition")
		defer span.End()

		// reader, err := storage.Reader(ctx, config.DatabaseFile)
		// if err != nil {
		// 	return tracing.Error(span, err)
		// }

		// book, err := openlibrary.GetBookByID(ctx, reader, r.PathValue("id"))
		// if err != nil {
		// 	return tracing.Error(span, err)
		// }

		// isbn := r.PathValue("isbn")
		// edition := book.Edition(isbn)
		// if edition == nil {
		// 	return tracing.Errorf(span, "no matching edition for isbn: %s", isbn)
		// }

		form, err := routing.Form(r)
		if err != nil {
			return err
		}
		dto := map[string]any{
			"QueryParams": form,
			// "Book":        book,
			// "Edition":     edition,
		}

		w.Header().Set("Content-Type", "text/html")
		if err := engine.Render(r.Context(), "catalogue/book.html", dto, w); err != nil {
			return err
		}

		return nil
	}))

	return nil
}
