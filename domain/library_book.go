package domain

import (
	"time"
)

type LibraryBook struct {
	BookInfo

	ID int

	Added time.Time
	Tags  []string
	State string
}
