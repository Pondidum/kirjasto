package ui

import (
	"context"
	"kirjasto/template"
	"net/http"
	"path"
)

func StaticFilesHandler(fs template.FS) func(ctx context.Context, mux *http.ServeMux, engine *template.TemplateEngine) error {
	return func(ctx context.Context, mux *http.ServeMux, engine *template.TemplateEngine) error {
		mux.Handle("/static/", WithPrefix("/common", http.FileServerFS(fs)))
		return nil
	}
}

func WithPrefix(prefix string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = path.Join(prefix, r.URL.Path)
		r.URL.RawPath = path.Join(prefix, r.URL.RawPath)

		h.ServeHTTP(w, r)
	})
}
