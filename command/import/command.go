package importcmd

import (
	"context"
	"database/sql"
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

	if err := WorksTables(ctx, db); err != nil {
		return tracing.Error(span, err)
	}

	//add this opt if using the debugger tea.WithInput(nil)
	prg := tea.NewProgram(&model{})

	go c.importWorks(ctx, db, prg.Send)
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

	for author, err := range Authors(f) {
		if err != nil {
			notify(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		count, err := importAuthor(ctx, *author)
		if err != nil {
			notify(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		notify(recordProcessed{changed: count > 0})
	}

	notify(fileImported{})

	return nil
}

func (c *ImportCommand) importWorks(ctx context.Context, db *sql.DB, notify func(msg tea.Msg)) error {
	ctx, span := tr.Start(ctx, "import_works")
	defer span.End()

	importWork, close, err := importWorkCommand(ctx, db)
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer close()

	f, err := os.Open(".data/openlibrary/ol_dump_works_2025-02-11.txt")
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer f.Close()

	for work, err := range Works(f) {
		if err != nil {
			notify(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		count, err := importWork(ctx, *work)
		if err != nil {
			notify(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		notify(recordProcessed{changed: count > 0})
		break
	}

	notify(fileImported{})

	return nil
}
