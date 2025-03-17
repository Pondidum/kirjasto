package storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
)

type Author struct {
	ID   string
	Name string
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
			book.Authors = append(book.Authors, Author{
				ID: author.Author.Key,
			})
		}

		books = append(books, book)
	}

	return books, nil
}
