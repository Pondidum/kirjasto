package domain

import (
	"context"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"slices"
	"strings"
	"time"
)

type LibraryView struct {
	Books []*LibraryEntry
}

type LibraryEntry struct {
	*openlibrary.Book

	Added time.Time
	Tags  []string
	State string

	KnownBook bool
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
	goes.AddProjectionHandler(projection.SqlProjection, projection.onBookAdded)

	return projection
}

func (p *LibraryProjection) onLibraryCreated(ctx context.Context, view *LibraryView, event LibraryCreated) error {
	return nil
}

func (p *LibraryProjection) onBookAdded(ctx context.Context, view *LibraryView, event BookAdded) error {
	le, err := p.createLibraryEntry(ctx, event.Book)
	if err != nil {
		return err
	}

	le.Tags = event.Tags
	le.Added = event.DateAdded

	view.Books = append(view.Books, le)
	return nil
}

func (p *LibraryProjection) onBookImported(ctx context.Context, view *LibraryView, event BookImported) error {
	le, err := p.createLibraryEntry(ctx, event.Book)
	if err != nil {
		return err
	}

	if !event.DateRead.IsZero() {
		le.State = "read"
	}

	le.Tags = event.Tags
	le.Added = event.DateAdded

	view.Books = append(view.Books, le)

	return nil
}

func (p *LibraryProjection) createLibraryEntry(ctx context.Context, info BookInfo) (*LibraryEntry, error) {
	book, err := p.findBook(ctx, info)
	if err != nil {
		return nil, err
	}

	le := &LibraryEntry{
		Book:      book,
		State:     "unread",
		KnownBook: book != nil,
	}

	if le.Book == nil {
		le.Book = &openlibrary.Book{
			Title:   info.Title,
			Authors: []openlibrary.Author{{Name: info.Author}},
			Isbns:   info.Isbns,
		}
		if info.PublishYear != 0 {
			ts := time.Date(info.PublishYear, 0, 0, 0, 0, 0, 0, time.UTC)
			le.Book.PublishDate = &ts
		}
	}

	return le, nil
}

func (p *LibraryProjection) findBook(ctx context.Context, info BookInfo) (*openlibrary.Book, error) {

	isbns := info.Isbns
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

	cleaned := strings.ReplaceAll(info.Title, ":", "")
	books, err := openlibrary.FindBooks(ctx, p.Tx, cleaned)
	if err != nil {
		return nil, err
	}
	if len(books) != 0 {
		return books[0], nil
	}

	return nil, nil
}
