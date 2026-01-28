package domain

import (
	"fmt"
	"kirjasto/goes"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("domain")

var LibraryID uuid.UUID = uuid.MustParse("89ea74d8-1960-41cc-b795-2d843f02c0aa")

func NewLibrary() *Library {
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

func CreateLibrary(id uuid.UUID) *Library {
	library := NewLibrary()

	goes.Apply(library, LibraryCreated{
		ID: id,
	})

	return library
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

	return bookImport.BookId, goes.Apply(l, bookImport)
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

	return added.BookId, goes.Apply(l, added)
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

	return goes.Apply(l, BookInfoAdded{BookId: bookId, Info: info})
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

	return goes.Apply(l, BookStarted{
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

	return goes.Apply(l, BookFinished{
		Isbn: isbn,
		When: when,
	})
}

func (l *Library) onBookFinished(e BookFinished) {
	// ?
}
