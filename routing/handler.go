package routing

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/template"
	"net/http"
)

type Handler = func(
	ctx context.Context,
	config *config.Config,
	mux *http.ServeMux,
	engine *template.TemplateEngine,
) error

func RouteHandler(h func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			fmt.Println("Error", err.Error())

			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
		}
	}
}
