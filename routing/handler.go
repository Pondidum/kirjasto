package routing

import (
	"context"
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
