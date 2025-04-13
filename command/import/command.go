package importcmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
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

	importAuthor, close, err := importAuthorCommand(ctx, db)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer close()

	f, err := os.Open(".data/openlibrary/ol_dump_works_2025-02-11.txt")
	if err != nil {
		return tracing.Error(span, err)
	}
	defer f.Close()

	fmt.Println("")
	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.LazyQuotes = true
	for {

		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return tracing.Error(span, err)
		}

		id := line[fieldId]
		dto := authorDto{}

		if err := json.Unmarshal([]byte(line[fieldJson]), &dto); err != nil {
			return tracing.Errorf(span, "error parsing %s: %w", line[fieldId], err)
		}

		if err := importAuthor(ctx, id, dto); err != nil {
			return tracing.Error(span, err)
		}
		fmt.Print(".")
	}
	fmt.Println("")
	fmt.Println("Done")

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
