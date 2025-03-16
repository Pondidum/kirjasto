package importcmd

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
	Exec  func(ctx context.Context, data T) (int, error)
	Close func(ctx context.Context) error
}

func importRecordCommand(ctx context.Context, db *sql.DB) (*InsertAction[OpenLibraryRecord], error) {

	statement, err := db.PrepareContext(ctx, `
		insert into openlibrary (id, type, revision, modified, data)
			values (?, ?, ?, ?, jsonb(?))
			on conflict(id) do update set
				revision = excluded.revision,
				modified = excluded.modified,
				data = excluded.data
			where excluded.revision > openlibrary.revision
	`)
	if err != nil {
		return nil, err
	}

	return &InsertAction[OpenLibraryRecord]{
		Exec: func(ctx context.Context, r OpenLibraryRecord) (int, error) {
			result, err := statement.ExecContext(ctx, r.ID, r.Type, r.Revision, r.Modified, r.Data)
			if err != nil {
				return 0, err
			}
			affected, _ := result.RowsAffected()
			return int(affected), nil
		},
		Close: func(ctx context.Context) error {
			return statement.Close()
		},
	}, nil
}
