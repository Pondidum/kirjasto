package domain

import (
	"context"
	"database/sql"
	"kirjasto/goes"
)

func NewStore(ctx context.Context, db *sql.DB) (*goes.SqliteStore, error) {
	store := goes.NewSqliteStore(db)

	if err := store.Initialise(ctx); err != nil {
		return nil, err
	}
	return store, nil
}
