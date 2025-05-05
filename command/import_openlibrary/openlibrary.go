package import_openlibrary

import (
	"encoding/csv"
	"io"
	"iter"
)

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

type editionDto struct {
	Record
	Isbn10 []string `json:"isbn_10"`
	Isbn13 []string `json:"isbn_13"`

	Authors []Record
}

const (
	fieldType = iota
	fieldId
	fieldVersion
	fieldModified
	fieldJson
)
