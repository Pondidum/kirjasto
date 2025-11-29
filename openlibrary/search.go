package openlibrary

import (
	"context"
	"slices"
	"strings"
)

func FindBook(ctx context.Context, reader Readable, title string, isbns []string) (*Book, error) {

	// prefer longer isbns
	slices.SortFunc(isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, isbn := range isbns {
		books, err := FindBooksByIsbn(ctx, reader, isbn)
		if err != nil {
			return nil, err
		}
		if len(books) != 0 {
			return books[0], nil
		}
	}

	cleaned := strings.ReplaceAll(title, ":", "")
	books, err := FindBooks(ctx, reader, cleaned)
	if err != nil {
		return nil, err
	}
	if len(books) != 0 {
		return books[0], nil
	}

	return nil, nil
}
