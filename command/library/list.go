package library

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/storage"
	"kirjasto/tracing"
	"kirjasto/util/columnize"

	"github.com/spf13/pflag"
)

func NewListCommand() *ListCommand {
	return &ListCommand{}
}

type ListCommand struct {
	tags []string
}

func (c *ListCommand) Synopsis() string {
	return "list the library contents"
}

func (c *ListCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("list", pflag.ContinueOnError)
	return flags
}

func (c *ListCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	reader, err := storage.Reader(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	p := domain.NewLibraryProjection()
	library, err := p.View(ctx, reader, domain.LibraryID)
	if err != nil {
		return tracing.Error(span, err)
	}

	rows := make([]string, 0, len(library.Books)+1)
	rows = append(rows, "isbn | title | author")

	for _, book := range library.Books {
		author := ""
		if len(book.Authors) > 0 {
			author = book.Authors[0].Name
		}
		rows = append(rows, fmt.Sprintf("%s | %s | %s", book.Isbns[0], book.Title, author))
	}

	fmt.Println(columnize.SimpleFormat(rows))

	return nil
}
