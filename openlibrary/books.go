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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

func bookResultRows(rows *sql.Rows) iter.Seq[bookResult] {
	return func(yield func(bookResult) bool) {

		rank := -1

		for rows.Next() {
			var editionJson string
			var authorJson string
			err := rows.Scan(&editionJson, &authorJson)

			if !yield(bookResult{rank, editionJson, authorJson, err}) {
				break
			}
		}
	}
}

func buildResults(ctx context.Context, rows iter.Seq[bookResult]) ([]*Book, error) {
	ctx, span := tr.Start(ctx, "build_results")
	defer span.End()

	groups := map[string][]*Book{}

	for bookRow := range rows {
		if bookRow.err != nil {
			return nil, tracing.Error(span, bookRow.err)
		}

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
				rank:           bookRow.rank,
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
