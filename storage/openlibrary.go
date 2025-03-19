package storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"kirjasto/tracing"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
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
	cmd := exec.Command("rg", "--fixed-strings", query, ".data/openlibrary/ol_dump_works_2025-02-11.txt")

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

	idMatch := workIdRx.MatchString(query)

	books := []Book{}
	for {

		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		dto := bookDto{}
		if err := json.Unmarshal([]byte(line[fieldJson]), &dto); err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", line[fieldId], err)
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
			author, err := GetAuthor(ctx, author.Author.Key)
			if err != nil {
				continue
			}

			book.Authors = append(book.Authors, author)
		}

		books = append(books, book)
	}

	return books, nil
}

func GetAuthor(ctx context.Context, id string) (Author, error) {
	ctx, span := tr.Start(ctx, "get_author")
	defer span.End()

	cacheDir := ".data/cache/authors/"
	if err := os.MkdirAll(cacheDir, os.ModePerm); err != nil {
		return Author{}, tracing.Error(span, err)
	}

	cacheKey := path.Base(id)

	if content, err := os.ReadFile(path.Join(cacheDir, cacheKey)); err == nil {
		return authorFromJson(content)
	}

	content, err := GetAuthorUncached(ctx, id)
	if err != nil {
		return Author{}, err
	}

	if err := os.WriteFile(path.Join(cacheDir, cacheKey), content, 0666); err != nil {
		span.RecordError(err)
	}

	return authorFromJson(content)

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

func GetAuthorUncached(ctx context.Context, id string) ([]byte, error) {

	query := fmt.Sprintf("\t%s\t", id)
	cmd := exec.Command("rg", "--fixed-strings", query, ".data/openlibrary/ol_dump_authors_2025-02-11.txt")

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

	line, err := reader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("no author found with id %s", id)
	}
	if err != nil {
		return nil, err
	}

	return []byte(line[fieldJson]), nil

}
