package import_openlibrary

import (
	"context"
	"database/sql"
)

func EditionsTables(ctx context.Context, writer *sql.DB) error {

	tables := `
		create table if not exists editions (
			isbn text primary key,
			data blob
		)
	`

	if _, err := writer.ExecContext(ctx, tables); err != nil {
		return err
	}

	return nil
}

type importEdition = func(ctx context.Context, isbn string, content []byte) (int64, error)

func importEditionsCommand(writer *sql.Tx) (importEdition, closer, error) {

	editions, err := writer.Prepare(`
		insert into
			editions (isbn, data)
			values   (@isbn, @data)
		on conflict(isbn) do update set
			data     = excluded.data
	`)
	if err != nil {
		return nil, nil, err
	}

	insert := func(ctx context.Context, isbn string, content []byte) (int64, error) {
		result, err := editions.ExecContext(
			ctx,
			sql.Named("isbn", isbn),
			sql.Named("data", content),
		)
		if err != nil {
			return 0, err
		}

		return result.RowsAffected()
	}

	return insert, editions.Close, nil
}
