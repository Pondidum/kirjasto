package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"kirjasto/tracing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type Author struct {
	ID   string
	Name string
}

type Book struct {
	ID       string
	Created  time.Time
	Modified time.Time
	Revision int
	Title    string
	Authors  []Author
	Covers   []int
}

func FindBookByTitle(ctx context.Context, reader *sql.DB, term string) ([]Book, error) {
	ctx, span := tr.Start(ctx, "find_book_by_title")
	defer span.End()

	span.SetAttributes(attribute.String("term", term))

	query := `
		select
			w.id,
			w.created,
			w.modified,
			w.revision,
			w.title,
			(
				select json_group_array(json_object('id', a.id, 'name', a.name))
				from json_each(w.authors) j
				join authors a on a.id == json_extract(j.value, '$.key')
			) as 'authors',
			w.covers,
			highlight(works_fts, 1, '{{', '}}') as 'match'
		from works_fts fts
		join works w on w.id = fts.id
		where fts.title match @term
	`

	rows, err := reader.QueryContext(ctx, query, sql.Named("term", term))
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	books := []Book{}
	for rows.Next() {
		book := Book{}
		var authorsJson []byte
		var coversJson []byte
		var match string

		if err := rows.Scan(&book.ID, &book.Created, &book.Modified, &book.Revision, &book.Title, &authorsJson, &coversJson, &match); err != nil {
			return nil, tracing.Error(span, err)
		}

		if err := json.Unmarshal([]byte(coversJson), &book.Covers); err != nil {
			return nil, tracing.Error(span, err)
		}

		if err := json.Unmarshal(authorsJson, &book.Authors); err != nil {
			return nil, tracing.Error(span, err)
		}

		books = append(books, book)
	}

	return books, nil
}
