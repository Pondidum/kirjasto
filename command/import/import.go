package importcmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"
	"strconv"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"

	tea "github.com/charmbracelet/bubbletea"
)

var tr = otel.Tracer("command.import")

func NewImportCommand() *ImportCommand {
	return &ImportCommand{}
}

type ImportCommand struct {
	send func(msg tea.Msg)
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

	prg := tea.NewProgram(&model{})
	c.send = prg.Send

	go c.importFile(ctx, config, sourceFile)
	if _, err := prg.Run(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportCommand) importFile(ctx context.Context, config *config.Config, sourceFile string) error {
	ctx, span := tr.Start(ctx, "import_file")
	defer span.End()

	lineCount, err := c.validateFile(ctx, sourceFile)
	if err != nil {
		c.send(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	c.send(validatedFile{lineCount})

	f, err := os.Open(sourceFile)
	if err != nil {
		c.send(recordProcessed{err: err})
		return tracing.Error(span, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		c.send(recordProcessed{err: err})
		return tracing.Error(span, err)
	}

	if err := storage.CreateTables(ctx, writer); err != nil {
		c.send(recordProcessed{err: err})
		return tracing.Error(span, err)
	}

	insert, err := importRecordCommand(ctx, writer)
	if err != nil {
		c.send(recordProcessed{err: err})
		return tracing.Error(span, err)
	}

	record := OpenLibraryRecord{}

	for {

		line, err := reader.Read()
		if err == io.EOF {
			c.send(fileImported{})
			return nil
		}
		if err != nil {
			c.send(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		record.Type = line[0]

		record.ID = line[1]
		if record.Revision, err = strconv.Atoi(line[2]); err != nil {
			c.send(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		if record.Modified, err = time.Parse("2006-01-02T15:04:05.999999", line[3]); err != nil {
			c.send(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		record.Data = line[4]

		count, err := insert.Exec(ctx, record)
		if err != nil {
			c.send(recordProcessed{err: err})
			return tracing.Error(span, err)
		}

		c.send(recordProcessed{changed: count > 0})
	}
}

func (c *ImportCommand) validateFile(ctx context.Context, sourceFile string) (int, error) {
	ctx, span := tr.Start(ctx, "validate_file")
	defer span.End()

	f, err := os.Open(sourceFile)
	if err != nil {
		return 0, tracing.Error(span, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	line, err := reader.Read()
	if err == io.EOF {
		return 0, nil
	}
	if err != nil {
		return 0, tracing.Error(span, err)
	}

	recordType := line[0]
	if recordType != "/type/work" && recordType != "/type/author" {
		return 0, tracing.Errorf(span, "file doesn't seem to be a works or author dump")
	}

	// count the lines, start at 1 as we already read one line using the csv reader
	buf := make([]byte, 32*1024)
	count := 1
	lineSep := []byte{'\n'}

	for {
		c, err := f.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return 0, err
		}
	}
}
