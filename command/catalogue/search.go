package catalogue

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/openlibrary"
	"kirjasto/storage"
	"kirjasto/tracing"
	"kirjasto/util/columnize"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("command.catalogue")

func NewSearchCommand() *SearchCommand {
	return &SearchCommand{}
}

type SearchCommand struct {
}

func (c *SearchCommand) Synopsis() string {
	return "search the catalogue for books"
}

func (c *SearchCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("search", pflag.ContinueOnError)
	return flags
}

func (c *SearchCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command expects 1 argument, but recieved %d", len(args))
	}

	searchTerm := args[0]

	reader, err := storage.Reader(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	books, err := openlibrary.FindBooks(ctx, reader, searchTerm)
	if err != nil {
		return tracing.Error(span, err)
	}

	lines := make([]string, 0, len(books)+1)
	lines = append(lines, "Isbn | Title | Author")

	for _, book := range books {

		author := ""
		if len(book.Authors) > 0 {
			author = book.Authors[0].Name
		}

		lines = append(lines, fmt.Sprintf(" %s | %s | %s", book.Isbns[0], book.Title, author))
	}

	fmt.Println(columnize.SimpleFormat(lines))

	return nil
}
