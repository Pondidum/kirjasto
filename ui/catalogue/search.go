package catalogue

import (
	"context"
	"database/sql"
	"encoding/json"
)

const selectBook string = `
select
	json(works.data)
from
	openlibrary works
where
	works.id = ?
limit ?
`

func QueryBooks(ctx context.Context, reader *sql.DB, pageSize int, query string) ([]Book, error) {

	rows, err := reader.QueryContext(ctx, selectBook, query, pageSize)
	if err != nil {
		return nil, err
	}

	books := []Book{}
	for rows.Next() {
		book := Book{}
		if err := rows.Scan(&book); err != nil {
			return nil, err
		}
		books = append(books, book)
	}

	return books, nil
}

type Book struct {
	BookId  string
	Name    string
	Authors []string
}

func (b *Book) Scan(value any) error {
	if value == nil {
		return nil
	}

	dto := struct {
		Title   string `json:"title"`
		Key     string `json:"key"`
		Authors []struct {
			Type   string `json:"type"`
			Author struct {
				Key string `json:"key"`
			}
		}
	}{}

	if err := json.Unmarshal([]byte(value.(string)), &dto); err != nil {
		return err
	}

	b.BookId = dto.Key
	b.Name = dto.Title
	for _, author := range dto.Authors {
		b.Authors = append(b.Authors, author.Author.Key)
	}

	return nil
}
