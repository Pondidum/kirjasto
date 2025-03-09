package storage

import (
	"context"
	"database/sql"
	"time"
)

type OpenLibraryRecord struct {
	ID       string
	Type     string
	Revision int
	Modified time.Time
	Data     string
}

type InsertAction[T any] struct {
	Exec  func(ctx context.Context, data T) error
	Close func(ctx context.Context) error
}

func InsertOpenLibrary(ctx context.Context, db *sql.DB) (*InsertAction[OpenLibraryRecord], error) {

	statement, err := db.PrepareContext(ctx, `insert into openlibrary (id, type, revision, modified, data) values (?, ?, ?, ?, jsonb(?))`)
	if err != nil {
		return nil, err
	}

	return &InsertAction[OpenLibraryRecord]{
		Exec: func(ctx context.Context, r OpenLibraryRecord) error {
			if _, err := statement.ExecContext(ctx, r.ID, r.Type, r.Revision, r.Modified, r.Data); err != nil {
				return err
			}
			return nil
		},
		Close: func(ctx context.Context) error {
			return statement.Close()
		},
	}, nil
}
