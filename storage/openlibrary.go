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
	ID       string
	Created  time.Time
	Modified time.Time
	Revision int
	Name     string
}

type Work struct {
	ID       string
	Created  time.Time
	Modified time.Time
	Revision int
	Title    string
	Authors  []Author
	Covers   []int
}

type Match struct {
	Work
	Match string
}

func GetWorkByID(ctx context.Context, reader *sql.DB, id string) (*Work, error) {
	ctx, span := tr.Start(ctx, "get_work")
	defer span.End()

	span.SetAttributes(attribute.String("work.id", id))

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
			w.covers
		from works w
		where w.id = @id
	`

	rows := reader.QueryRowContext(ctx, query, sql.Named("id", id))

	book := Work{}
	var authorsJson []byte
	var coversJson []byte

	err := rows.Scan(&book.ID, &book.Created, &book.Modified, &book.Revision, &book.Title, &authorsJson, &coversJson)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	if err := json.Unmarshal([]byte(coversJson), &book.Covers); err != nil {
		return nil, tracing.Error(span, err)
	}

	if err := json.Unmarshal(authorsJson, &book.Authors); err != nil {
		return nil, tracing.Error(span, err)
	}

	return &book, nil

}

func GetAuthorByID(ctx context.Context, reader *sql.DB, id string) (*Author, error) {
	ctx, span := tr.Start(ctx, "get_author")
	defer span.End()

	span.SetAttributes(attribute.String("author.id", id))

	query := `
	select
			id,
			created,
			modified,
			revision,
			name
		from authors
		where id = @id
	`

	rows := reader.QueryRowContext(ctx, query, sql.Named("id", id))

	author := Author{}

	err := rows.Scan(&author.ID, &author.Created, &author.Modified, &author.Revision, &author.Name)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, tracing.Error(span, err)
	}

	return &author, nil
}

func FindWorkByTitle(ctx context.Context, reader *sql.DB, term string) ([]Match, error) {
	ctx, span := tr.Start(ctx, "find_work_by_title")
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

	matches := []Match{}
	for rows.Next() {
		book := Work{}
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

		matches = append(matches, Match{
			Work:  book,
			Match: match,
		})
	}

	return matches, nil
}
