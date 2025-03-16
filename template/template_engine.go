package template

import (
	"context"
	"io"
	"io/fs"
	"kirjasto/tracing"
	"path"
	"strings"
	"text/template"

	"github.com/go-viper/mapstructure/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tr = otel.Tracer("template")

type FS interface {
	fs.ReadFileFS
	fs.ReadDirFS
}

type TemplateEngine struct {
	source    FS
	options   EngineOptions
	templates map[string]*template.Template
}

type EngineOptions struct {
	HotReload bool
}

func NewTemplateEngine(source FS, options EngineOptions) *TemplateEngine {
	return &TemplateEngine{
		source:    source,
		options:   options,
		templates: map[string]*template.Template{},
	}
}

func (te *TemplateEngine) ParseTemplates(ctx context.Context) error {
	ctx, span := tr.Start(ctx, "parse_templates")
	defer span.End()

	dir, err := te.source.ReadDir("common")
	if err != nil {
		return tracing.Error(span, err)
	}

	commonContents := &strings.Builder{}
	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}

		content, err := te.source.ReadFile(path.Join("common", entry.Name()))
		if err != nil {
			return tracing.Error(span, err)
		}
		commonContents.Write(content)
	}

	dir, err = te.source.ReadDir(".")
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

		files, err := te.source.ReadDir(entry.Name())
		if err != nil {
			return tracing.Error(span, err)
		}

		for _, file := range files {
			name := path.Join(entry.Name(), file.Name())
			content, err := te.source.ReadFile(name)
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

func (te *TemplateEngine) Render(ctx context.Context, template string, data any, writer io.Writer) error {
	ctx, span := tr.Start(ctx, "render")
	defer span.End()

	tpl, found := te.templates[template]

	span.SetAttributes(
		attribute.String("template.name", template),
		attribute.Bool("template.exists", found),
	)

	if !found {
		return tracing.Errorf(span, "no template called %s found", template)
	}

	fields := map[string]any{}
	if err := mapstructure.Decode(data, &fields); err != nil {
		return tracing.Error(span, err)
	}

	fields["Engine"] = te.options

	if err := tpl.ExecuteTemplate(writer, "base", fields); err != nil {
		return tracing.Error(span, err)
	}
	return nil
}
