package domain

import (
	"kirjasto/openlibrary"
	"time"

	"github.com/google/uuid"
)

type LibraryCreated struct {
	ID uuid.UUID
}

type BookImported struct {
	Book Book

	Tags      []string
	Rating    int
	ReadCount int

	DateAdded time.Time
	DateRead  time.Time
}

type BookAdded struct {
	Book      Book
	Tags      []string
	DateAdded time.Time
}

type Book struct {
	Isbns       []string
	Title       string
	Author      string
	PublishYear int
}

type BookInfoAdded struct {
	Info openlibrary.Book
}

type BookStarted struct {
	Isbn string
	When time.Time
}

type BookFinished struct {
	Isbn string
	When time.Time
}
