package storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"kirjasto/tracing"
	"os/exec"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

type Author struct {
	ID   string
	Name string
}

type authorDto struct {
	Key            string
	Name           string
	Revision       int
	LatestRevision int `json:"latest_revision"`
}

type Book struct {
	ID      string
	Name    string
	Authors []Author
	Covers  []int
}

type bookDto struct {
	Key            string
	Title          string
	Covers         []int
	Revision       int
	LatestRevision int `json:"latest_revision"`
	Authors        []struct {
		Author struct {
			Key string
		}
	}
}

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)

var workIdRx = regexp.MustCompile(`^OL\d*W$`)

func FindBookByTitle(ctx context.Context, query string) ([]Book, error) {
	ctx, span := tr.Start(ctx, "find_book_by_title")
	defer span.End()

	span.SetAttributes(attribute.String("query", query))

	cmd := exec.Command("rg", "--fixed-strings", query, ".data/openlibrary/ol_dump_works_2025-02-11.txt")

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 1 {
				span.SetAttributes(attribute.Int("search.results_count", 0))
				return []Book{}, nil
			}
		}

		return nil, tracing.Errorf(span, "err: %w\nstdout:%s\nstderr:%s", err, stdout.String(), stderr.String())
	}

	reader := csv.NewReader(stdout)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	idMatch := workIdRx.MatchString(query)

	authorIds := map[string]bool{}
	books := []Book{}
	for {

		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, tracing.Error(span, err)
		}

		dto := bookDto{}
		if err := json.Unmarshal([]byte(line[fieldJson]), &dto); err != nil {
			return nil, tracing.Errorf(span, "error parsing %s: %w", line[fieldId], err)
		}

		// double check it was a title match
		if idMatch {
			if !strings.EqualFold(dto.Key, "/works/"+query) {
				continue
			}
		} else {
			if !strings.Contains(dto.Title, query) {
				continue
			}
		}

		book := Book{
			ID:     dto.Key,
			Name:   dto.Title,
			Covers: dto.Covers,
		}

		for _, author := range dto.Authors {
			authorIds[author.Author.Key] = true
			book.Authors = append(book.Authors, Author{
				ID: author.Author.Key,
			})
		}

		books = append(books, book)
	}

	span.SetAttributes(attribute.Int("search.results_count", len(books)))

	authors, err := GetAllAuthors(ctx, authorIds)
	if err != nil {
		return books, tracing.Error(span, err)
	}

	for _, book := range books {

		ba := make([]Author, 0, len(book.Authors))
		for _, a := range book.Authors {
			if full, found := authors[a.ID]; found {
				ba = append(ba, full)
			}
		}
		book.Authors = ba
	}

	return books, nil
}

func GetAllAuthors(ctx context.Context, ids map[string]bool) (map[string]Author, error) {
	ctx, span := tr.Start(ctx, "get_all_authors")
	defer span.End()

	args := make([]string, 0, (len(ids)*2)+2)
	args = append(args, "--fixed-strings")

	for id := range ids {
		args = append(args, "-e", id)
	}
	args = append(args, ".data/openlibrary/ol_dump_authors_2025-02-11.txt")

	cmd := exec.Command("rg", args...)

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf(stderr.String(), err)
	}

	reader := csv.NewReader(stdout)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	authors := make(map[string]Author, len(ids))
	for {
		line, err := reader.Read()
		if err == io.EOF {
			return authors, nil
		}
		if err != nil {
			return nil, err
		}

		author, err := authorFromJson([]byte(line[fieldJson]))
		if err != nil {
			span.RecordError(err)
			continue
		}

		authors[author.ID] = author
	}
}

func authorFromJson(content []byte) (Author, error) {
	dto := authorDto{}
	if err := json.Unmarshal(content, &dto); err != nil {
		return Author{}, fmt.Errorf("error parsing %w\n%s", err, string(content))
	}

	return Author{
		ID:   dto.Key,
		Name: dto.Name,
	}, nil
}
