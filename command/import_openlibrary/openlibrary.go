package import_openlibrary

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
)

func Authors(r io.Reader) iter.Seq2[*authorDto, error] {
	return iterateRecords[authorDto](r)
}

func Works(r io.Reader) iter.Seq2[*workDto, error] {
	return iterateRecords[workDto](r)
}

func Editions(r io.Reader) iter.Seq2[[]byte, error] {
	return iterateFile(r)
}

func iterateRecords[T any](r io.Reader) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		for content, err := range iterateFile(r) {
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}

			dto := new(T)

			if err := json.Unmarshal(content, &dto); err != nil {
				if !yield(nil, fmt.Errorf("error parsing json: %w", err)) {
					return
				}
			}

			if !yield(dto, nil) {
				return
			}
		}

	}
}

func iterateFile(r io.Reader) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {

		reader := csv.NewReader(r)
		reader.Comma = '\t'
		reader.LazyQuotes = true
		for {

			line, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}

			content := line[fieldJson]
			if !yield([]byte(content), nil) {
				return
			}

		}

	}
}

type Record struct {
	Key     string
	Created struct {
		Value string
	} `json:"created"`
	Modified struct {
		Value string
	} `json:"last_modified"`
	Revision int
}

type workDto struct {
	Record
	Title   string
	Covers  []int
	Authors []authorLink
}

type authorLink struct {
	//type
	Key string `json:"key"`
}

func (al *authorLink) UnmarshalJSON(data []byte) error {

	normal := &struct {
		Author struct{ Key string }
	}{}

	if err := json.Unmarshal(data, normal); err == nil {
		al.Key = normal.Author.Key
		return nil
	}

	stringBased := &struct {
		Author string
	}{}

	if err := json.Unmarshal(data, stringBased); err == nil {
		al.Key = stringBased.Author
		return nil
	}

	return errors.New("unable to parse the authors")
}

type authorDto struct {
	Record
	Name string
}

type editionDto struct {
	Record
	Isbn10 []string `json:"isbn_10"`
	Isbn13 []string `json:"isbn_13"`
}

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)
