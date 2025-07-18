package openlibrary

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"kirjasto/tracing"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tr = otel.Tracer("openlibrary")

type Book struct {
	Isbns []string

	Title    string
	Subtitle string
	Authors  []Author
	Covers   []int // ??

	PublishDate *time.Time

	OtherEditions  []*Book
	rank           int
	openLibraryKey string
}

type Author struct {
	ID   string
	Name string
}

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

func bookResultRows(rows *sql.Rows) iter.Seq[bookResult] {
	return func(yield func(bookResult) bool) {

		var editionJson string
		var authorJson string

		for rows.Next() {
			err := rows.Scan(&editionJson, &authorJson)

			if !yield(bookResult{editionJson, authorJson, err}) {
				break
			}
		}
	}
}

type bookResult struct {
	editionJson string
	authorJson  string
	err         error
}

func buildResults(ctx context.Context, rows iter.Seq[bookResult]) ([]*Book, error) {
	ctx, span := tr.Start(ctx, "build_results")
	defer span.End()

	groups := map[string][]*Book{}

	rank := -1

	for bookRow := range rows {
		if bookRow.err != nil {
			return nil, tracing.Error(span, bookRow.err)
		}

		rank++

		authors, err := authorsFromJson(bookRow.authorJson)
		if err != nil {
			return nil, tracing.Error(span, err)
		}

		editionDto := editionDto{}
		if err := json.Unmarshal([]byte(bookRow.editionJson), &editionDto); err != nil {
			return nil, tracing.Error(span, err)
		}

		for _, work := range editionDto.Works {

			book := &Book{
				Title:          editionDto.Title,
				Subtitle:       editionDto.Subtitle,
				Isbns:          append(editionDto.Isbn13, editionDto.Isbn10...),
				Authors:        authors,
				Covers:         editionDto.Covers,
				rank:           rank,
				openLibraryKey: editionDto.Key,
			}

			if len(book.Isbns) == 0 {
				continue
			}

			if publishDate, err := parsePublishDate(editionDto.PublishDate); err == nil {
				book.PublishDate = &publishDate
			} else {
				span.AddEvent("date_parsing_failed", trace.WithAttributes(
					attribute.String("edition.publish_date", editionDto.PublishDate),
				))
			}

			groups[work.Key] = append(groups[work.Key], book)
		}

	}

	books := make([]*Book, 0, len(groups))

	for _, group := range groups {

		slices.SortFunc(group, func(a, b *Book) int { return a.rank - b.rank })
		main := group[0]
		main.OtherEditions = group[1:]

		books = append(books, main)
	}

	slices.SortFunc(books, func(a, b *Book) int { return a.rank - b.rank })

	return books, nil
}

func parsePublishDate(publishDate string) (time.Time, error) {

	if parsed, err := time.Parse("Jan _2, 2006", publishDate); err == nil {
		return parsed, nil
	}

	if parsed, err := time.Parse("2006", publishDate); err == nil {
		return parsed, nil
	}

	return time.Time{}, fmt.Errorf("No format matched for '%s'", publishDate)
}

func authorsFromJson(authorJson string) ([]Author, error) {

	dto := []authorDto{}
	if err := json.Unmarshal([]byte(authorJson), &dto); err != nil {
		return nil, err
	}

	authors := make([]Author, len(dto))
	for i, src := range dto {
		authors[i] = Author{
			ID:   src.Key,
			Name: src.Name,
		}
	}
	return authors, nil
}

type editionDto struct {
	Key string

	Title          string
	Subtitle       string
	PhysicalFormat string `json:"physical_format"`

	PublishDate string `json:"publish_date"`

	Isbn10 []string `json:"isbn_10"`
	Isbn13 []string `json:"isbn_13"`

	Covers []int
	Works  []workDto
}

type authorDto struct {
	Key  string
	Name string
}

type workDto struct {
	Key string `json:"key"`
}
