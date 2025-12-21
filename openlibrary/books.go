package openlibrary

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

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

type bookResult struct {
	rank        int
	editionJson string
	authorJson  string
	err         error
}

func asBooks(rows *sql.Rows) ([]*Book, error) {
	books := []*Book{}

	rank := -1

	for rows.Next() {
		rank++

		var editionJson string
		var authorJson string

		if err := rows.Scan(&editionJson, &authorJson); err != nil {
			return nil, err
		}

		book, err := asBook(rank, editionJson, authorJson)
		if err != nil {
			return nil, err
		}

		books = append(books, book)
	}

	return books, nil
}

func asBook(rank int, editionJson string, authorJson string) (*Book, error) {

	authors, err := authorsFromJson(authorJson)
	if err != nil {
		return nil, err
	}

	editionDto := editionDto{}
	if err := json.Unmarshal([]byte(editionJson), &editionDto); err != nil {
		return nil, err
	}

	book := &Book{
		Title:          editionDto.Title,
		Subtitle:       editionDto.Subtitle,
		Isbns:          append(editionDto.Isbn13, editionDto.Isbn10...),
		Authors:        authors,
		Covers:         editionDto.Covers,
		rank:           rank,
		openLibraryKey: editionDto.Key,
	}

	if publishDate, err := parsePublishDate(editionDto.PublishDate); err == nil {
		book.PublishDate = &publishDate
	}

	return book, nil
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

type workDto struct {
	Key string `json:"key"`
}
