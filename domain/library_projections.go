package domain

import (
	"context"
	"fmt"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"slices"
	"strings"
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

	book, err := p.findBook(ctx, event)
	if err != nil {
		return err
	}

	if book == nil {
		fmt.Println("no book found for:", strings.Join(event.Isbns, ","), event.Title)
		return nil
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

func (p *LibraryProjection) findBook(ctx context.Context, event BookImported) (*openlibrary.Book, error) {

	isbns := event.Isbns
	// prefer longer isbns
	slices.SortFunc(isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, isbn := range isbns {
		books, err := openlibrary.FindBooksByIsbn(ctx, p.Tx, isbn)
		if err != nil {
			return nil, err
		}
		if len(books) != 0 {
			return books[0], nil
		}
	}

	books, err := openlibrary.FindBooks(ctx, p.Tx, event.Title)
	if err != nil {
		return nil, err
	}
	if len(books) != 0 {
		return books[0], nil
	}

	return nil, nil
}
