package domain

import (
	"context"
	"fmt"
	"kirjasto/goes"
	"kirjasto/tracing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("domain")

var LibraryID uuid.UUID = uuid.MustParse("89ea74d8-1960-41cc-b795-2d843f02c0aa")

func blankLibrary() *Library {
	library := &Library{
		AggregateState: goes.NewAggregateState(),
	}

	goes.Register(library.AggregateState, library.onLibraryCreated)
	goes.Register(library.AggregateState, library.onBookImported)
	goes.Register(library.AggregateState, library.onBookAdded)
	goes.Register(library.AggregateState, library.onBookStarted)
	goes.Register(library.AggregateState, library.onBookFinished)
	goes.Register(library.AggregateState, library.onBookInfoAdded)

	return library
}

func NewLibrary(id uuid.UUID) *Library {
	library := blankLibrary()

	goes.Apply(library.AggregateState, LibraryCreated{
		ID: id,
	})

	return library
}

func LoadLibrary(ctx context.Context, eventStore *goes.SqliteStore, id uuid.UUID) (*Library, error) {
	ctx, span := tr.Start(ctx, "load_library")
	defer span.End()

	library := blankLibrary()
	goes.SetID(library.AggregateState, id)

	if err := goes.Load(ctx, eventStore, library.AggregateState); err != nil {
		return nil, tracing.Error(span, err)
	}

	return library, nil
}

type Library struct {
	*goes.AggregateState

	Books      []*LibraryBook
	nextBookId int
}

func (l *Library) onLibraryCreated(e LibraryCreated) {
	goes.SetID(l.AggregateState, e.ID)
}

type ImportData struct {
	Added     time.Time
	Read      time.Time
	ReadCount int
	Tags      []string
	Extra     map[string]any
}

func (l *Library) ImportBook(info BookInfo, importData ImportData) (int, error) {

	bookImport := BookImported{
		BookId:     l.nextBookId,
		Book:       info,
		ImportData: importData,
	}

	return bookImport.BookId, goes.Apply(l.AggregateState, bookImport)
}

func (l *Library) onBookImported(e BookImported) {
	l.nextBookId++
	lb := &LibraryBook{
		ID:       e.BookId,
		BookInfo: e.Book,
		Added:    e.ImportData.Added,
		Tags:     e.ImportData.Tags,
	}
	if !e.ImportData.Read.IsZero() || e.ImportData.ReadCount > 0 {
		lb.State = "read"
	}

	l.Books = append(l.Books, lb)
}

func (l *Library) AddBook(book BookInfo, tags []string) (int, error) {
	added := BookAdded{
		BookId:    l.nextBookId,
		Book:      book,
		Tags:      tags,
		DateAdded: time.Now(),
	}

	return added.BookId, goes.Apply(l.AggregateState, added)
}

func (l *Library) onBookAdded(e BookAdded) {
	l.nextBookId++
	lb := &LibraryBook{
		ID:       e.BookId,
		BookInfo: e.Book,
		Added:    e.DateAdded,
		Tags:     e.Tags,
	}

	l.Books = append(l.Books, lb)
}

func (l *Library) AddBookInfo(bookId int, info BookInfo) error {

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

	return goes.Apply(l.AggregateState, BookInfoAdded{BookId: bookId, Info: info})
}

func (l *Library) onBookInfoAdded(e BookInfoAdded) {
	for _, book := range l.Books {
		if book.ID == e.BookId {
			book.BookInfo = e.Info
			return
		}
	}
}

func (l *Library) StartReading(isbn string, when time.Time) error {
	if when.IsZero() {
		when = time.Now()
	}

	return goes.Apply(l.AggregateState, BookStarted{
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

	return goes.Apply(l.AggregateState, BookFinished{
		Isbn: isbn,
		When: when,
	})
}

func (l *Library) onBookFinished(e BookFinished) {
	// ?
}
