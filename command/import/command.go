package importcmd

import (
	"context"
	"database/sql"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"
	"path"
	"strings"

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

	if len(args) != 1 {
		return tracing.Errorf(span, "this command takes exactly 1 argument: a path to import")
	}
	filePath := args[0]

	fileType, err := c.detectFileType(ctx, filePath)
	if err != nil {
		return tracing.Error(span, err)
	}

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

	file, err := os.Open(filePath)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer file.Close()

	//add this opt if using the debugger tea.WithInput(nil)
	prg := tea.NewProgram(&model{})

	switch fileType {
	case "works":
		go c.importWorks(ctx, db, file, prg.Send)
	case "authors":
		go c.importAuthors(ctx, db, file, prg.Send)
	default:
		return tracing.Errorf(span, "only 'work' and 'author' records are supported, received '%s'", fileType)
	}

	if _, err := prg.Run(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportCommand) detectFileType(ctx context.Context, filePath string) (string, error) {
	ctx, span := tr.Start(ctx, "detect_file_type")
	defer span.End()

	file, err := os.Open(filePath)
	if err != nil {
		return "", tracing.Error(span, err)
	}
	defer file.Close()

	iterator := iterateFile[Record](file)

	for record, err := range iterator {
		if err != nil {
			return "", tracing.Error(span, err)
		}
		return strings.Trim(path.Dir(record.Key), "/"), nil
	}

	return "", tracing.Errorf(span, "no records found in file")
}

func (c *ImportCommand) importAuthors(ctx context.Context, db *sql.DB, reader io.Reader, notify func(msg tea.Msg)) error {
	ctx, span := tr.Start(ctx, "import_authors")
	defer span.End()

	importAuthor, close, err := importAuthorCommand(db)
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer close()

	for author, err := range Authors(reader) {
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

func (c *ImportCommand) importWorks(ctx context.Context, db *sql.DB, reader io.Reader, notify func(msg tea.Msg)) error {
	ctx, span := tr.Start(ctx, "import_works")
	defer span.End()

	importWork, err := importWorkCommand(db)
	if err != nil {
		notify(recordProcessed{err: err})
		return tracing.Error(span, err)
	}

	for work, err := range Works(reader) {
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
