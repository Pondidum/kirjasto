package domain

import (
	"kirjasto/openlibrary"
	"time"
)

type LibraryBook struct {
	Book

	ID int

	Added time.Time
	Tags  []string
	State string
}

func (lb *LibraryBook) Apply(info openlibrary.Book) {
	lb.Book.Title = info.Title
	names := make([]string, len(info.Authors))
	for i, author := range info.Authors {
		names[i] = author.Name
	}

	lb.Book.Authors = names
	lb.Book.Isbns = info.Isbns
	lb.Book.PublishYear = info.PublishDate.Year()
}

type Book struct {
	Isbns       []string
	Title       string
	Authors     []string
	PublishYear int
}
