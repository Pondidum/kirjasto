package openlibrary

import (
	"context"
	"database/sql"
	"kirjasto/tracing"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("openlibrary")

type Readable interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func FindBooksByIsbn(ctx context.Context, reader Readable, isbn string) ([]*Book, error) {
	ctx, span := tr.Start(ctx, "find_book_by_isbn")
	defer span.End()

	query := `
		select
			e.data,
			(
				select json_group_array(json(a.data))
				from editions_authors_link eal
				join authors a on a.id = eal.author_id
				where eal.edition_id  = e.id
			)
		from editions e
		join editions_isbns_link eil on eil.edition_id = e.id
		where eil.isbn = @isbn
	`

	rows, err := reader.QueryContext(ctx, query, sql.Named("isbn", isbn))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	results := bookResultRows(rows)
	books, err := buildResults(ctx, results)
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return books, nil
}

func FindBooks(ctx context.Context, reader Readable, search string) ([]*Book, error) {
	ctx, span := tr.Start(ctx, "find_books")
	defer span.End()

	query := `
		select
			e.data,
			(
				select json_group_array(json(a.data))
				from editions_authors_link eal
				join authors a on a.id = eal.author_id
				where eal.edition_id  = e.id
			) --,
			-- highlight(editions_fts, 1, '{{', '}}')
		from editions e
		join editions_fts fts on e.id = fts.edition_id
		where editions_fts match @term
		order by rank
	`

	rows, err := reader.QueryContext(ctx, query, sql.Named("term", search))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	results := bookResultRows(rows)
	books, err := buildResults(ctx, results)
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return books, nil
}
