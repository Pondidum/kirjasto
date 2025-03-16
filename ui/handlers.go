package ui

import (
	"context"
	"embed"
	"io/fs"
	"kirjasto/config"
	"kirjasto/routing"
	"kirjasto/template"
	"kirjasto/tracing"
	"kirjasto/ui/catalogue"
	"net/http"
	"os"
	"path"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("ui")

//go:embed */*
var staticFiles embed.FS

func RegisterUI(ctx context.Context, cfg *config.Config, server *http.ServeMux) error {
	ctx, span := tr.Start(ctx, "register_handlers")
	defer span.End()

	hasExternal := hasExternalFiles()
	handlers := []routing.Handler{}

	var fs template.FS

	if hasExternal {
		fs = os.DirFS("./ui").(template.FS)
	} else {
		fs = staticFiles
	}

	engine := template.NewTemplateEngine(fs, template.EngineOptions{
		HotReload: hasExternal,
	})
	if err := engine.ParseTemplates(ctx); err != nil {
		return tracing.Error(span, err)
	}

	if hasExternal {
		hr := HotReload(fs)
		handlers = append(handlers, hr.Register)

		err := StartWatching(ctx, allFolders(fs),
			func(path string) error { return engine.ParseTemplates(ctx) },
			func(path string) error { return hr.Reload(ctx) },
		)
		if err != nil {
			return tracing.Error(span, err)
		}

	}

	handlers = append(handlers, StaticFilesHandler(fs))

	// app areas
	handlers = append(handlers, catalogue.RegisterHandlers)

	for _, handler := range handlers {
		if err := handler(ctx, cfg, server, engine); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}

func hasExternalFiles() bool {
	const baseTemplate string = "./ui/common/base.html"

	if _, err := os.Stat(baseTemplate); os.IsNotExist(err) {
		return false
	}
	return true
}

func allFolders(fsys template.FS) []string {

	all := []string{}

	fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			all = append(all, path.Join("./ui", p))
		}
		return nil
	})

	return all
}
