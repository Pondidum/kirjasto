package importcmd

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"

	tea "github.com/charmbracelet/bubbletea"
)

var tr = otel.Tracer("command.import")

func NewImportCommand() *ImportCommand {
	return &ImportCommand{}
}

type ImportCommand struct {
}

func (c *ImportCommand) Synopsis() string {
	return "import something or other"
}

func (c *ImportCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("server", pflag.ContinueOnError)
	return flags
}

func (c *ImportCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	db, err := storage.Writer(ctx, "fts.sqlite")
	if err != nil {
		return tracing.Error(span, err)
	}
	defer db.Close()

	if err := AuthorsTables(ctx, db); err != nil {
		return tracing.Error(span, err)
	}

	prg := tea.NewProgram(&model{})
	// c.send = prg.Send

	go c.importAuthors(ctx, db, prg.Send)
	if _, err := prg.Run(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportCommand) importAuthors(ctx context.Context, db *sql.DB, notify func(msg tea.Msg)) error {
	ctx, span := tr.Start(ctx, "import_authors")
	defer span.End()

	importAuthor, close, err := importAuthorCommand(ctx, db)
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer close()

	f, err := os.Open(".data/openlibrary/ol_dump_authors_2025-02-11.txt")
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.LazyQuotes = true
	for {

		line, err := reader.Read()
		if err == io.EOF {
			notify(fileImported{})
			break
		}
		if err != nil {
			notify(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		id := line[fieldId]
		dto := authorDto{}

		if err := json.Unmarshal([]byte(line[fieldJson]), &dto); err != nil {
			notify(recordProcessed{err: err})
			return tracing.Errorf(span, "error parsing %s: %w", line[fieldId], err)
		}

		count, err := importAuthor(ctx, id, dto)
		if err != nil {
			return tracing.Error(span, err)
		}

		notify(recordProcessed{changed: count > 0})
	}

	return nil
}

type authorDto struct {
	Created struct {
		Value string
	} `json:"created"`
	Modified struct {
		Value string
	} `json:"last_modified"`
	Revision int
	Name     string
}

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)
