package importcmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"
	"strconv"
	"time"

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
	return "import some data"
}

func (c *ImportCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("import", pflag.ContinueOnError)
	return flags
}

func (c *ImportCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command takes one argument: source file")
	}
	sourceFile := args[0]

	//for now, we are assuming this is an openlibrary datadump

	f, err := os.Open(sourceFile)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := storage.CreateTables(ctx, writer); err != nil {
		return tracing.Error(span, err)
	}

	insert, err := storage.InsertOpenLibrary(ctx, writer)
	if err != nil {
		return tracing.Error(span, err)
	}

	record := storage.OpenLibraryRecord{}

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return tracing.Error(span, err)
		}

		record.Type = line[0]

		if record.Type != "/type/work" && record.Type != "/type/author" {
			return tracing.Errorf(span, "file doesn't seem to be a works or author dump")
		}

		record.ID = line[1]
		if record.Revision, err = strconv.Atoi(line[2]); err != nil {
			return tracing.Error(span, err)
		}

		if record.Modified, err = time.Parse("2006-01-02T15:04:05.999999", line[3]); err != nil {
			return tracing.Error(span, err)
		}

		record.Data = line[4]

		if err := insert.Exec(ctx, record); err != nil {
			return tracing.Error(span, err)
		}
		fmt.Print(".")
	}

	fmt.Println("")
	return nil
}
