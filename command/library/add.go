package library

import (
	"context"
	"fmt"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/openlibrary"
	"kirjasto/tracing"
	"time"

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

	authors     []string
	publishYear int
	tags        []string
}

func (c *AddCommand) Name() string {
	return "library add"
}

func (c *AddCommand) Synopsis() string {
	return "add a book to the library"
}

func (c *AddCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("add", pflag.ContinueOnError)
	flags.StringSliceVar(&c.book.Isbns, "isbn", []string{}, "the book's isbn(s)")
	flags.StringVar(&c.book.Title, "title", "", "the book's title")
	flags.StringSliceVar(&c.authors, "author", []string{}, "the book's authors")
	flags.IntVar(&c.publishYear, "publish-year", 0, "the year the book was published")
	flags.StringSliceVar(&c.tags, "tags", []string{}, "tags to add to the book")
	return flags
}

func (c *AddCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(c.book.Isbns) == 0 && c.book.Title == "" {
		return tracing.Errorf(span, "The books must have at least one of: isbn, title")
	}

	if c.publishYear > 0 {
		c.book.PublishDate = time.Date(c.publishYear, 0, 0, 0, 0, 0, 0, time.Now().Location())
	}

	c.book.Authors = make([]domain.Author, len(c.authors))
	for i, author := range c.authors {
		c.book.Authors[i] = domain.Author{
			Source:   "manual",
			SourceId: author,
			Name:     author,
		}
	}

	c.book.Source = "manual"
	c.book.SourceId = "none"

	openlib, err := openlibrary.NewClient(ctx, config.OpenLibraryDb)
	if err != nil {
		return tracing.Error(span, err)
	}
	bookInfo, err := openlib.FindBook(ctx, c.book.Title, c.book.Isbns)
	if err != nil {
		return tracing.Error(span, err)
	}

	if bookInfo == nil {
		fmt.Println("Unable to find a matchin book")
		return nil
	}

	err = libraryAction(ctx, config, func(library *domain.Library) error {
		bookId, err := library.AddBook(c.book, c.tags)
		if err != nil {
			return err
		}

		if bookInfo != nil {
			if err := library.AddBookInfo(bookId, *bookInfo); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return tracing.Error(span, err)
	}

	return nil
}
