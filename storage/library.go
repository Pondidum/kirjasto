package storage

import (
	"context"
	"database/sql"
	"kirjasto/goes"
	"kirjasto/tracing"
	"time"

	"github.com/google/uuid"
)

var LibraryID uuid.UUID = uuid.MustParse("89ea74d8-1960-41cc-b795-2d843f02c0aa")

func blankLibrary() *Library {
	library := &Library{
		state:      goes.NewAggregateState(),
		knownIsbns: map[string]bool{},
	}

	goes.Register(library.state, library.onLibraryCreated)
	goes.Register(library.state, library.onBookImported)

	return library
}

func NewLibrary(id uuid.UUID) *Library {
	library := blankLibrary()

	goes.Apply(library.state, LibraryCreated{
		ID: id,
	})

	return library
}

func LoadLibrary(ctx context.Context, db *sql.DB, id uuid.UUID) (*Library, error) {
	ctx, span := tr.Start(ctx, "load_library")
	defer span.End()

	library := blankLibrary()

	if err := goes.Load(ctx, db, library.state, id); err != nil {
		return nil, tracing.Error(span, err)
	}

	return library, nil
}

func SaveLibrary(ctx context.Context, db *sql.DB, library *Library) error {
	return goes.Save(ctx, db, library.state)
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

type BookImport struct {
	Isbns     []string
	Rating    int
	ReadCount int
	Shelves   []string

	DateAdded time.Time
	DateRead  time.Time
}

type BookImported struct {
	Isbns     []string
	Rating    int
	ReadCount int
	Shelves   []string

	DateAdded time.Time
	DateRead  time.Time
}

func (l *Library) ImportBook(info BookImport) error {

	for _, isbn := range info.Isbns {
		if _, found := l.knownIsbns[isbn]; found {
			return nil
		}
	}

	return goes.Apply(l.state, BookImported{
		Isbns:     info.Isbns,
		Rating:    info.Rating,
		ReadCount: info.ReadCount,
		Shelves:   info.Shelves,

		DateAdded: info.DateAdded,
		DateRead:  info.DateRead,
	})
}

func (l *Library) onBookImported(e BookImported) {

	for _, isbn := range e.Isbns {
		l.knownIsbns[isbn] = true
	}
}
