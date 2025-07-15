package domain

import (
	"context"
	"kirjasto/goes"
	"kirjasto/openlibrary"
	"slices"
)

type LibraryView struct {
	Tags  map[string]struct{}
	Books []*openlibrary.Book
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

	for _, isbn := range event.Isbns {
		books, err := openlibrary.FindBooksByIsbn(ctx, p.Tx, isbn)
		if err != nil {
			return err
		}

		if len(books) == 0 {
			continue
		}

		view.Books = append(view.Books, books[0])
		break
	}

	return nil
}
