package importcmd

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
)

func Authors(r io.Reader) iter.Seq2[*authorDto, error] {
	return iterateFile[authorDto](r)
}

func Works(r io.Reader) iter.Seq2[*workDto, error] {
	return iterateFile[workDto](r)
}

func iterateFile[T any](r io.Reader) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {

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

			dto := new(T)

			if err := json.Unmarshal([]byte(line[fieldJson]), &dto); err != nil {
				if !yield(nil, fmt.Errorf("error parsing %s: %w", line[fieldId], err)) {
					return
				}
			}
			if !yield(dto, nil) {
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
	Key string
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

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)
