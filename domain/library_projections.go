package domain

import (
	"context"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"time"
)

type LibraryView struct {
	Books  []*LibraryEntry
	ByIsbn map[string]int
}

func (v *LibraryView) addLibraryEntry(le *LibraryEntry) {
	index := len(v.Books)
	v.Books = append(v.Books, le)
	for _, isbn := range le.Isbns {
		v.ByIsbn[isbn] = index
	}
}

func (v *LibraryView) forBook(isbns []string) []*LibraryEntry {
	entries := make([]*LibraryEntry, 0, len(isbns))

	for _, isbn := range isbns {
		if index, found := v.ByIsbn[isbn]; found {
			entries = append(entries, v.Books[index])
		}
	}

	return entries
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
	goes.AddProjectionHandler(projection.SqlProjection, projection.onBookInfoAdded)

	return projection
}

func (p *LibraryProjection) onLibraryCreated(ctx context.Context, view *LibraryView, event LibraryCreated) error {
	return nil
}

func (p *LibraryProjection) onBookAdded(ctx context.Context, view *LibraryView, event BookAdded) error {
	le := newLibraryEntry(event.Book)
	le.Tags = event.Tags
	le.Added = event.DateAdded

	view.addLibraryEntry(le)
	return nil
}

func (p *LibraryProjection) onBookImported(ctx context.Context, view *LibraryView, event BookImported) error {
	le := newLibraryEntry(event.Book)
	le.Tags = event.Tags
	le.Added = event.DateAdded

	if !event.DateRead.IsZero() {
		le.State = "read"
	}

	view.addLibraryEntry(le)
	return nil
}

func (p *LibraryProjection) onBookInfoAdded(ctx context.Context, view *LibraryView, event BookInfoAdded) error {
	entries := view.forBook(event.Info.Isbns)

	for _, le := range entries {
		le.Book = event.Info
	}

	return nil
}

func newLibraryEntry(book Book) *LibraryEntry {

	le := &LibraryEntry{
		Book: &openlibrary.Book{
			Title:   book.Title,
			Authors: []openlibrary.Author{{Name: book.Author}},
			Isbns:   book.Isbns,
		},
		State:     "unread",
		KnownBook: false,
	}

	if book.PublishYear != 0 {
		ts := time.Date(book.PublishYear, 0, 0, 0, 0, 0, 0, time.UTC)
		le.Book.PublishDate = &ts
	}

	return le
}
