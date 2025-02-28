package ui

import (
	"bytes"
	"context"
	"embed"
	"kirjasto/tracing"
	"path"
	"strings"
	"text/template"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tr = otel.Tracer("ui")

//go:embed */*.html
var templatesSource embed.FS

type TemplateEngine struct {
	templates map[string]*template.Template
}

func NewTemplateEngine() *TemplateEngine {
	return &TemplateEngine{
		templates: map[string]*template.Template{},
	}
}

func (te *TemplateEngine) ParseTemplates(ctx context.Context) error {
	ctx, span := tr.Start(ctx, "parse_templates")
	defer span.End()

	dir, err := templatesSource.ReadDir("common")
	if err != nil {
		return tracing.Error(span, err)
	}

	commonContents := &strings.Builder{}
	for _, entry := range dir {
		content, err := templatesSource.ReadFile(path.Join("common", entry.Name()))
		if err != nil {
			return tracing.Error(span, err)
		}
		commonContents.Write(content)
	}

	dir, err = templatesSource.ReadDir(".")
	if err != nil {
		return tracing.Error(span, err)
	}

	for _, entry := range dir {
		if !entry.IsDir() {
			continue
		}

		// special handling for this dir
		if entry.Name() == "common" {
			continue
		}

		files, err := templatesSource.ReadDir(entry.Name())
		if err != nil {
			return tracing.Error(span, err)
		}

		for _, file := range files {
			name := path.Join(entry.Name(), file.Name())
			content, err := templatesSource.ReadFile(name)
			if err != nil {
				return tracing.Error(span, err)
			}

			combined := strings.Builder{}
			combined.WriteString(commonContents.String())
			combined.Write(content)

			tpl, err := template.New("main").Parse(combined.String())
			if err != nil {
				return tracing.Error(span, err)
			}

			te.templates[name] = tpl
		}
	}

	return nil
}

func (te *TemplateEngine) Render(ctx context.Context, template string) ([]byte, error) {
	ctx, span := tr.Start(ctx, "render")
	defer span.End()

	tpl, found := te.templates[template]

	span.SetAttributes(
		attribute.String("template.name", template),
		attribute.Bool("template.exists", found),
	)

	if !found {
		return nil, tracing.Errorf(span, "no template called %s found", template)
	}

	b := &bytes.Buffer{}
	if err := tpl.ExecuteTemplate(b, "base", map[string]any{}); err != nil {
		return nil, tracing.Error(span, err)
	}

	return b.Bytes(), nil
}
