package importcmd

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
	id string primary key,
	created timestamp,
	modified timestamp,
	revision integer,
	name string
);
`

	if _, err := writer.ExecContext(ctx, tables); err != nil {
		return err
	}

	return nil
}

type importAuthor = func(ctx context.Context, id string, author authorDto) error
type closer = func() error

func importAuthorCommand(ctx context.Context, writer *sql.DB) (importAuthor, closer, error) {
	statement, err := writer.PrepareContext(ctx, `
		insert into
			authors (id, created, modified, revision, name)
			values  (@id, @created, @modified, @revision, @name);
		on conflict(id) do update set
			created  = excluded.created,
			modified = excluded.modified,
			revision = excluded.revision,
			name    = excluded.name
		where excluded.revision > authors.revision;

		insert into
			authors_fts (id, name)
			values 			(@id, @name)
		on conflict(id) do update set
			name = excluded.name;
	`)
	if err != nil {
		return nil, nil, err
	}

	insert := func(ctx context.Context, id string, author authorDto) error {
		_, err := statement.ExecContext(
			ctx,
			sql.Named("id", id),
			sql.Named("created", author.Created.Value),
			sql.Named("modified", author.Modified.Value),
			sql.Named("revision", author.Revision),
			sql.Named("name", author.Name),
		)
		return err
	}

	return insert, statement.Close, nil
}
