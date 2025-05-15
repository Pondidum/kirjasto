package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"kirjasto/tracing"
	"maps"
	"path"
	"slices"

	"go.opentelemetry.io/otel/attribute"
)

type Book struct {
	ID       string
	Editions []*Edition

	editions map[string]*Edition
}

func (b *Book) Edition(isbn string) *Edition {
	if e, found := b.editions[isbn]; found {
		return e
	}

	return nil
}

type Edition struct {
	Isbns []string

	Title    string
	Subtitle string
	Authors  []Author
	Covers   []int // ??
}

type Author struct {
	ID   string
	Name string
}

func GetBookByID(ctx context.Context, reader *sql.DB, id string) (*Book, error) {
	ctx, span := tr.Start(ctx, "get_book_by_id")
	defer span.End()

	span.SetAttributes(attribute.String("book.id", id))

	workId := fmt.Sprintf("/works/%s", id)
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
		join editions_works_link ewl on e.id = ewl.edition_id
		where ewl.work_id  = @id
	`

	rows, err := reader.QueryContext(ctx, query, sql.Named("id", workId))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	books, err := buildBooks(ctx, rows)
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	if book, found := books[id]; found {
		return &book, nil
	}

	return nil, nil

}

func FindBooks(ctx context.Context, reader *sql.DB, search string) ([]Book, error) {
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
		where fts.title match @term
	`

	rows, err := reader.QueryContext(ctx, query, sql.Named("term", search))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	books, err := buildBooks(ctx, rows)
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return slices.Collect(maps.Values(books)), nil
}

func buildBooks(ctx context.Context, rows *sql.Rows) (map[string]Book, error) {
	ctx, span := tr.Start(ctx, "build_books")
	defer span.End()

	books := map[string]Book{}

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, tracing.Error(span, err)
		}

		editionJson := ""
		authorJson := ""

		if err := rows.Scan(&editionJson, &authorJson); err != nil {
			return nil, tracing.Error(span, err)
		}

		editionDto := editionDto{}
		if err := json.Unmarshal([]byte(editionJson), &editionDto); err != nil {
			return nil, tracing.Error(span, err)
		}

		authors := []authorDto{}
		if err := json.Unmarshal([]byte(authorJson), &authors); err != nil {
			return nil, tracing.Error(span, err)
		}

		edition := &Edition{
			Title:    editionDto.Title,
			Subtitle: editionDto.Subtitle,
			Isbns:    append(editionDto.Isbn10, editionDto.Isbn13...),
			Authors:  authorsFromJson(authors),
		}

		for _, w := range editionDto.Works {
			bookId := path.Base(w.Key)
			book, exists := books[bookId]
			if !exists {
				book = Book{
					ID:       bookId,
					editions: map[string]*Edition{},
				}
				books[book.ID] = book
			}

			book.Editions = append(book.Editions, edition)
			for _, isbn := range edition.Isbns {
				book.editions[isbn] = edition
			}

			books[book.ID] = book
		}

	}

	return books, nil
}

func authorsFromJson(dto []authorDto) []Author {

	authors := make([]Author, len(dto))
	for i, src := range dto {
		authors[i] = Author{
			ID:   src.Key,
			Name: src.Name,
		}
	}
	return authors
}

type editionDto struct {
	Key string

	Title          string
	Subtitle       string
	PhysicalFormat string `json:"physical_format"`

	Isbn10 []string `json:"isbn_10"`
	Isbn13 []string `json:"isbn_13"`

	Works []workDto
}

type authorDto struct {
	Key  string
	Name string
}

type workDto struct {
	Key string `json:"key"`
}
