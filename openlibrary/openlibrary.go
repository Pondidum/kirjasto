package openlibrary

import (
	"context"
	"database/sql"
	"kirjasto/domain"
	"kirjasto/storage"
	"kirjasto/tracing"
	"slices"

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

func (c *Client) FindBooksByIsbn(ctx context.Context, isbn string) ([]domain.BookInfo, error) {
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
		join editions_works_link ewl on ewl.edition_id = e.id
		join editions_isbns_link eil on eil.edition_id = e.id
		where eil.isbn = @isbn
		group by ewl.work_id
	`

	rows, err := c.reader.QueryContext(ctx, query, sql.Named("isbn", isbn))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return asBooks(rows)
}

func (c *Client) FindBooksByTitleAuthor(ctx context.Context, title, author string) ([]domain.BookInfo, error) {
	ctx, span := tr.Start(ctx, "find_book_by_title")
	defer span.End()

	query := `
	select 
		e."data",
		(
			select json_group_array(json(a.data))
			from editions_authors_link eal
			join authors a on a.id = eal.author_id
			where eal.edition_id  = e.id
		)
	from authors_fts af 
	join editions_authors_link eal on af.author_id = eal.author_id 
	join editions_fts ef on ef.edition_id = eal.edition_id 
	join editions_works_link ewl on ewl.edition_id = ef.edition_id 
	join editions e on e.id  = ewl.edition_id 
	where af.name  match @author
	  and editions_fts match @title
	group by ewl.work_id
	order by min(af.rank, ef.rank)`

	rows, err := c.reader.QueryContext(ctx, query, sql.Named("title", title), sql.Named("author", author))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return asBooks(rows)
}

func (c *Client) FindBooks(ctx context.Context, ref domain.BookInfo) ([]domain.BookInfo, error) {
	ctx, span := tr.Start(ctx, "find_book")
	defer span.End()

	// prefer longer isbns
	isbns := slices.SortedFunc(slices.Values(ref.Isbns), func(a, b string) int {
		return len(b) - len(a)
	})

	matches := []domain.BookInfo{}

	for _, isbn := range isbns {
		books, err := c.FindBooksByIsbn(ctx, isbn)
		if err != nil {
			return nil, err
		}
		matches = append(matches, books...)
	}

	if len(matches) > 0 {
		return matches, nil
	}

	cleanedTitle := storage.FtsEscape(ref.Title)

	for _, author := range ref.Authors {
		books, err := c.FindBooksByTitleAuthor(ctx, cleanedTitle, author.Name)
		if err != nil {
			return nil, err
		}

		matches = append(matches, books...)
	}

	return nil, nil
}
