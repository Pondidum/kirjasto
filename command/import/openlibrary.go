package importcmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"iter"
)

func Authors(r io.Reader) iter.Seq2[*authorDto, error] {
	return iterateFile[authorDto](r)
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

type authorDto struct {
	Key     string
	Created struct {
		Value string
	} `json:"created"`
	Modified struct {
		Value string
	} `json:"last_modified"`
	Revision int
	Name     string
}

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)
