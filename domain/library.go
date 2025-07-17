package domain

import (
	"context"
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
		state:      goes.NewAggregateState(),
		knownIsbns: map[string]bool{},
	}

	goes.Register(library.state, library.onLibraryCreated)
	goes.Register(library.state, library.onBookImported)
	goes.Register(library.state, library.onBookAdded)
	goes.Register(library.state, library.onBookStarted)
	goes.Register(library.state, library.onBookFinished)

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

	knownIsbns map[string]bool
}

type LibraryCreated struct {
	ID uuid.UUID
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

type BookImported struct {
	Isbns       []string
	Title       string
	Author      string
	PublishYear int

	Rating    int
	ReadCount int
	Tags      []string

	DateAdded time.Time
	DateRead  time.Time
}

func (l *Library) ImportBook(info ImportData) error {

	for _, isbn := range info.Isbns {
		if _, found := l.knownIsbns[isbn]; found {
			return nil
		}
	}

	return goes.Apply(l.state, BookImported{
		Isbns:       info.Isbns,
		Title:       info.Title,
		Author:      info.Author,
		PublishYear: info.PublishYear,

		Rating:    info.Rating,
		ReadCount: info.ReadCount,
		Tags:      info.Shelves,

		DateAdded: info.DateAdded,
		DateRead:  info.DateRead,
	})
}

func (l *Library) onBookImported(e BookImported) {

	for _, isbn := range e.Isbns {
		l.knownIsbns[isbn] = true
	}
}

type BookAdded struct {
	Isbns     []string
	Tags      []string
	DateAdded time.Time
}

func (l *Library) AddBook(isbns []string, tags []string) error {

	for _, isbn := range isbns {
		if _, found := l.knownIsbns[isbn]; found {
			return nil
		}
	}

	return goes.Apply(l.state, BookAdded{
		Isbns:     isbns,
		Tags:      tags,
		DateAdded: time.Now(),
	})
}

func (l *Library) onBookAdded(e BookAdded) {
	for _, isbn := range e.Isbns {
		l.knownIsbns[isbn] = true
	}
}

type BookStarted struct {
	Isbn string
	When time.Time
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

type BookFinished struct {
	Isbn string
	When time.Time
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
