package config

import (
	"context"
)

type Config struct {
	// DatabaseFile string

	OpenLibraryDb string
	LibraryDb     string
}

func CreateConfig(ctx context.Context) (*Config, error) {
	return &Config{
		// DatabaseFile:  "dev.sqlite",
		LibraryDb:     "dev.sqlite",
		OpenLibraryDb: "openlibrary.sqlite",
	}, nil
}
