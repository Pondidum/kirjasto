package domain

import (
	"time"

	"github.com/google/uuid"
)

type LibraryCreated struct {
	ID uuid.UUID
}

type BookImported struct {
	BookId     int
	Book       BookInfo
	ImportData ImportData
}

type BookAdded struct {
	BookId    int
	Book      BookInfo
	Tags      []string
	DateAdded time.Time
}

type BookInfoAdded struct {
	BookId int
	Info   BookInfo
}

type BookStarted struct {
	Isbn string
	When time.Time
}

type BookFinished struct {
	Isbn string
	When time.Time
}
