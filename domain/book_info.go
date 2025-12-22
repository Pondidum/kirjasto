package domain

import "time"

// usually supplied by a search provider, e.g. openlibrary
type BookInfo struct {
	Source   string
	SourceId string

	Isbns []string

	Title    string
	Subtitle string
	Authors  []Author

	PublishDate time.Time
}

type Author struct {
	Source   string
	SourceId string

	Name string
}
