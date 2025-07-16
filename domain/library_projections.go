package domain

import (
	"context"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"slices"
	"time"
)

type LibraryView struct {
	Tags  map[string]struct{}
	Books []*LibraryEntry
}

type LibraryEntry struct {
	*openlibrary.Book

	Added time.Time
	Tags  []string
	State string
}

type LibraryProjection struct {
	*goes.SqlProjection[LibraryView]
}

func NewLibraryProjection() *LibraryProjection {
	projection := &LibraryProjection{
		SqlProjection: goes.NewSqlProjection[LibraryView](),
	}

	goes.AddProjectionHandler(projection.SqlProjection, projection.onLibraryCreated)
	goes.AddProjectionHandler(projection.SqlProjection, projection.onBookImported)

	return projection
}

func (p *LibraryProjection) onLibraryCreated(ctx context.Context, view *LibraryView, event LibraryCreated) error {
	view.Tags = map[string]struct{}{}
	return nil
}

func (p *LibraryProjection) onBookImported(ctx context.Context, view *LibraryView, event BookImported) error {
	for _, tag := range event.Tags {
		view.Tags[tag] = struct{}{}
	}

	// prefer longer isbns
	slices.SortFunc(event.Isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	book, err := p.findBook(ctx, event.Isbns)
	if err != nil {
		return err
	}

	if book == nil {
		return err
	}

	state := "unread"
	if !event.DateRead.IsZero() {
		state = "read"
	}

	// view.Books = append(view.Books, book)
	view.Books = append(view.Books, &LibraryEntry{
		Book:  book,
		Added: event.DateAdded,
		Tags:  event.Tags,
		State: state,
	})

	return nil
}

func (p *LibraryProjection) findBook(ctx context.Context, isbns []string) (*openlibrary.Book, error) {

	// prefer longer isbns
	slices.SortFunc(isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, isbn := range isbns {
		books, err := openlibrary.FindBooksByIsbn(ctx, p.Tx, isbn)
		if err != nil {
			return nil, err
		}

		if len(books) == 0 {
			continue
		}

		return books[0], nil
	}

	return nil, nil
}
