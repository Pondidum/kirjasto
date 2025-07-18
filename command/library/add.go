package library

import (
	"context"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/goes"
	"kirjasto/storage"
	"kirjasto/tracing"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("command.library")

func NewAddCommand() *AddCommand {
	return &AddCommand{
		book: domain.BookInfo{},
	}
}

type AddCommand struct {
	book domain.BookInfo
	tags []string
}

func (c *AddCommand) Synopsis() string {
	return "add a book to the library"
}

func (c *AddCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("add", pflag.ContinueOnError)
	flags.StringSliceVar(&c.book.Isbns, "isbn", []string{}, "the book's isbn(s)")
	flags.StringVar(&c.book.Title, "title", "", "the book's title")
	flags.StringVar(&c.book.Author, "author", "", "the book's author")
	flags.IntVar(&c.book.PublishYear, "publish-year", 0, "the year the book was published")
	flags.StringSliceVar(&c.tags, "tags", []string{}, "tags to add to the book")
	return flags
}

func (c *AddCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(c.book.Isbns) == 0 && c.book.Title == "" {
		return tracing.Errorf(span, "The books must have at least one of: isbn, title")
	}

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	store := goes.NewSqliteStore(writer)

	if err := goes.RegisterProjection("library_view", domain.NewLibraryProjection()); err != nil {
		return tracing.Error(span, err)
	}

	library, err := domain.LoadLibrary(ctx, store, domain.LibraryID)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := library.AddBook(c.book, c.tags); err != nil {
		return tracing.Error(span, err)
	}

	if err := domain.SaveLibrary(ctx, store, library); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
