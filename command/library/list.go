package library

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/goes"
	"kirjasto/storage"
	"kirjasto/tracing"
	"kirjasto/util/columnize"

	"github.com/spf13/pflag"
)

func NewListCommand() *ListCommand {
	return &ListCommand{}
}

type ListCommand struct {
	statsOnly bool
}

func (c *ListCommand) Name() string {
	return "library list"
}

func (c *ListCommand) Synopsis() string {
	return "list the library contents"
}

func (c *ListCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("list", pflag.ContinueOnError)
	flags.BoolVar(&c.statsOnly, "stats", false, "print some stats and exit")
	return flags
}

func (c *ListCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	reader, err := storage.Reader(ctx, config.LibraryDb)
	if err != nil {
		return tracing.Error(span, err)
	}

	eventStore := goes.NewSqliteStore(reader)
	library := domain.NewLibrary()
	if err := eventStore.Load(ctx, domain.LibraryID, library); err != nil {
		return tracing.Error(span, err)
	}

	c.printStats(ctx, library)
	if c.statsOnly {
		return nil
	}

	rows := make([]string, 0, len(library.Books)+1)
	rows = append(rows, "isbn | state | title | added")

	for _, book := range library.Books {
		isbn := "unknown"
		if len(book.Isbns) > 0 {
			isbn = book.Isbns[0]
		}
		rows = append(rows, fmt.Sprintf("%s | %s | %s | %s", isbn, book.State, book.Title, book.Added.Format("2006-01-02")))
	}

	fmt.Println(columnize.SimpleFormat(rows))

	return nil
}

func (c *ListCommand) printStats(ctx context.Context, library *domain.Library) {
	read := 0
	unread := 0

	for _, book := range library.Books {
		switch book.State {
		case "read":
			read++
		case "unread":
			unread++
		}
	}

	fmt.Printf("Total books: %v (%v read, %v unread)\n", len(library.Books), read, unread)
}
