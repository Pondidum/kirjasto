package import_openlibrary

import (
	"context"
	"database/sql"
)

func AuthorsTables(ctx context.Context, writer *sql.DB) error {

	tables := `
create virtual table if not exists authors_fts using fts5 (
	id,
	name
);

create table if not exists authors (
	id text primary key,
	created timestamp,
	modified timestamp,
	revision integer,
	name text
);
`

	if _, err := writer.ExecContext(ctx, tables); err != nil {
		return err
	}

	return nil
}

type importAuthor = func(ctx context.Context, author authorDto) (int64, error)
type closer = func() error

func importAuthorCommand(writer *sql.Tx) (importAuthor, closer, error) {

	authors, err := writer.Prepare(`
		insert into
			authors (id, created, modified, revision, name)
			values  (@id, @created, @modified, @revision, @name)
		on conflict(id) do update set
			created  = excluded.created,
			modified = excluded.modified,
			revision = excluded.revision,
			name    = excluded.name
		where excluded.revision > authors.revision;
	`)
	if err != nil {
		return nil, nil, err
	}

	insert := func(ctx context.Context, author authorDto) (int64, error) {
		id := sql.Named("id", author.Key)
		name := sql.Named("name", author.Name)

		result, err := authors.ExecContext(
			ctx,
			id,
			sql.Named("created", author.Created.Value),
			sql.Named("modified", author.Modified.Value),
			sql.Named("revision", author.Revision),
			name,
		)
		if err != nil {
			return 0, err
		}

		return result.RowsAffected()
	}

	return insert, authors.Close, nil
}

func createAuthorIndexes(ctx context.Context, writer *sql.Tx) error {
	_, err := writer.ExecContext(ctx, `
		delete from authors_fts;

		insert into authors_fts(id, name)
		select id, name
		from authors
	`)
	if err != nil {
		return err
	}
	return nil
}
