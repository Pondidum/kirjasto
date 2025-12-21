package domain

import (
	"context"
	"fmt"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"kirjasto/tracing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("domain")

var LibraryID uuid.UUID = uuid.MustParse("89ea74d8-1960-41cc-b795-2d843f02c0aa")

func blankLibrary() *Library {
	library := &Library{
		state: goes.NewAggregateState(),
	}

	goes.Register(library.state, library.onLibraryCreated)
	goes.Register(library.state, library.onBookImported)
	goes.Register(library.state, library.onBookAdded)
	goes.Register(library.state, library.onBookStarted)
	goes.Register(library.state, library.onBookFinished)
	goes.Register(library.state, library.onBookInfoAdded)

	return library
}

func NewLibrary(id uuid.UUID) *Library {
	library := blankLibrary()

	goes.Apply(library.state, LibraryCreated{
		ID: id,
	})

	return library
}

func LoadLibrary(ctx context.Context, eventStore *goes.SqliteStore, id uuid.UUID) (*Library, error) {
	ctx, span := tr.Start(ctx, "load_library")
	defer span.End()

	library := blankLibrary()
	goes.SetID(library.state, id)

	if err := goes.Load(ctx, eventStore, library.state); err != nil {
		return nil, tracing.Error(span, err)
	}

	return library, nil
}

func SaveLibrary(ctx context.Context, eventStore *goes.SqliteStore, library *Library) error {
	return goes.Save(ctx, eventStore, library.state)
}

type Library struct {
	state *goes.AggregateState

	Books      []LibraryBook
	nextBookId int
}

func (l *Library) onLibraryCreated(e LibraryCreated) {
	goes.SetID(l.state, e.ID)
}

type ImportData struct {
	Isbns       []string
	Title       string
	Author      string
	PublishYear int

	Rating    int
	ReadCount int
	Shelves   []string

	DateAdded time.Time
	DateRead  time.Time
}

func (l *Library) ImportBook(info ImportData) (int, error) {

	bookImport := BookImported{
		BookId: l.nextBookId,
		Book: Book{
			Isbns:       info.Isbns,
			Title:       info.Title,
			Authors:     []string{info.Author},
			PublishYear: info.PublishYear,
		},

		Rating:    info.Rating,
		ReadCount: info.ReadCount,
		Tags:      info.Shelves,

		DateAdded: info.DateAdded,
		DateRead:  info.DateRead,
	}

	return bookImport.BookId, goes.Apply(l.state, bookImport)
}

func (l *Library) onBookImported(e BookImported) {
	l.nextBookId++
	lb := LibraryBook{
		ID:    e.BookId,
		Book:  e.Book,
		Added: e.DateAdded,
		Tags:  e.Tags,
	}
	if !e.DateRead.IsZero() {
		lb.State = "read"
	}

	l.Books = append(l.Books, lb)
}

func (l *Library) AddBook(book Book, tags []string) (int, error) {
	added := BookAdded{
		BookId:    l.nextBookId,
		Book:      book,
		Tags:      tags,
		DateAdded: time.Now(),
	}

	return added.BookId, goes.Apply(l.state, added)
}

func (l *Library) onBookAdded(e BookAdded) {
	l.nextBookId++
	lb := LibraryBook{
		ID:    e.BookId,
		Book:  e.Book,
		Added: e.DateAdded,
		Tags:  e.Tags,
	}

	l.Books = append(l.Books, lb)
}

func (l *Library) AddBookInfo(bookId int, info openlibrary.Book) error {

	found := false
	for _, lb := range l.Books {
		if lb.ID == bookId {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("no book found for id '%v'", bookId)
	}

	return goes.Apply(l.state, BookInfoAdded{BookId: bookId, Info: info})
}

func (l *Library) onBookInfoAdded(e BookInfoAdded) {
	for _, book := range l.Books {
		if book.ID == e.BookId {
			book.Apply(e.Info)
			return
		}
	}
}

func (l *Library) StartReading(isbn string, when time.Time) error {
	if when.IsZero() {
		when = time.Now()
	}

	return goes.Apply(l.state, BookStarted{
		Isbn: isbn,
		When: when,
	})
}

func (l *Library) onBookStarted(e BookStarted) {
	// ?
}

func (l *Library) FinishReading(isbn string, when time.Time) error {
	if when.IsZero() {
		when = time.Now()
	}

	return goes.Apply(l.state, BookFinished{
		Isbn: isbn,
		When: when,
	})
}

func (l *Library) onBookFinished(e BookFinished) {
	// ?
}
