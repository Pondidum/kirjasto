package openlibrary

import (
	"context"
	"database/sql"
	"iter"
	"kirjasto/storage"
	"kirjasto/tracing"
	"slices"
	"strings"

	"go.opentelemetry.io/otel"
)

var tr = otel.Tracer("openlibrary")

type Client struct {
	reader *sql.DB
}

func NewClient(ctx context.Context, dbPath string) (*Client, error) {
	reader, err := storage.Reader(ctx, dbPath)
	if err != nil {
		return nil, err
	}

	return &Client{
		reader: reader,
	}, nil
}

func (c *Client) FindBooksByIsbn(ctx context.Context, isbn string) (iter.Seq[*Book], error) {
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

	rows, err := c.reader.QueryContext(ctx, query, sql.Named("isbn", isbn))
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

func (c *Client) FindBooksByTitle(ctx context.Context, search string) ([]*Book, error) {
	ctx, span := tr.Start(ctx, "find_book_by_title")
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

	rows, err := c.reader.QueryContext(ctx, query, sql.Named("term", search))
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

func (c *Client) FindBook(ctx context.Context, title string, isbns []string) (*Book, error) {
	ctx, span := tr.Start(ctx, "find_book")
	defer span.End()

	// prefer longer isbns
	slices.SortFunc(isbns, func(a, b string) int {
		return len(b) - len(a)
	})

	for _, isbn := range isbns {
		books, err := c.FindBooksByIsbn(ctx, isbn)
		if err != nil {
			return nil, err
		}
		if len(books) != 0 {
			return books[0], nil
		}
	}

	cleaned := strings.ReplaceAll(title, ":", "")
	books, err := c.FindBooksByTitle(ctx, cleaned)
	if err != nil {
		return nil, err
	}
	if len(books) != 0 {
		return books[0], nil
	}

	return nil, nil
}
