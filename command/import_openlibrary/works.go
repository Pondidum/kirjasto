package import_openlibrary

import (
	"context"
	"database/sql"
	"encoding/json"
)

func WorksTables(ctx context.Context, writer *sql.DB) error {

	tables := `
create virtual table if not exists works_fts using fts5 (
	id,
	title
);

create table if not exists works (
	id text primary key,
	created timestamp,
	modified timestamp,
	revision integer,
	title text,
	covers blob,
	authors blob
);
`

	if _, err := writer.ExecContext(ctx, tables); err != nil {
		return err
	}

	return nil
}

type importWork = func(ctx context.Context, work workDto) (int64, error)

func importWorkCommand(writer *sql.Tx) (importWork, closer, error) {

	works, err := writer.Prepare(`
		insert into
			works (id, created, modified, revision, title, covers, authors)
			values  (@id, @created, @modified, @revision, @title, @covers, @authors)
		on conflict(id) do update set
			created  = excluded.created,
			modified = excluded.modified,
			revision = excluded.revision,
			title    = excluded.title,
			covers   = excluded.covers,
			authors  = excluded.authors
		where excluded.revision > works.revision;
	`)
	if err != nil {
		return nil, nil, err
	}

	insert := func(ctx context.Context, work workDto) (int64, error) {
		covers, err := json.Marshal(work.Covers)
		if err != nil {
			return 0, err
		}

		authors, err := json.Marshal(work.Authors)
		if err != nil {
			return 0, err
		}

		id := sql.Named("id", work.Key)
		title := sql.Named("title", work.Title)

		result, err := works.ExecContext(
			ctx,
			id,
			sql.Named("created", work.Created.Value),
			sql.Named("modified", work.Modified.Value),
			sql.Named("revision", work.Revision),
			title,
			covers,
			authors,
		)
		if err != nil {
			return 0, err
		}

		return result.RowsAffected()
	}

	return insert, works.Close, nil
}

func createWorksIndexes(ctx context.Context, writer *sql.Tx) error {
	_, err := writer.ExecContext(ctx, `
		delete from works_fts;

		insert into works_fts(id, title)
		select id, title
		from works
	`)
	if err != nil {
		return err
	}
	return nil
}
