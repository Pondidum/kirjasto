package importcmd

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
	covers blob
);

create table if not exists work_authors (
	work_id text,
	author_id text,
	foreign key (work_id) references works(id),
	foreign key (author_id) references authors(id)
);
`

	if _, err := writer.ExecContext(ctx, tables); err != nil {
		return err
	}

	return nil
}

type importWork = func(ctx context.Context, work workDto) (int64, error)

func importWorkCommand(ctx context.Context, writer *sql.DB) (importWork, closer, error) {
	statement, err := writer.PrepareContext(ctx, `
		select @id, @created, @modified, @revision, @title, @covers, @authors;

		insert into
			works (id, created, modified, revision, title, covers)
			values  (@id, @created, @modified, @revision, @title, @covers)
		on conflict(id) do update set
			created  = excluded.created,
			modified = excluded.modified,
			revision = excluded.revision,
			title    = excluded.title,
			covers   = excluded.covers
		where excluded.revision > works.revision;

		insert into
			works_fts (id, title)
			values 			(@id, @title)
		on conflict(id) do update set
			title = excluded.title;

		delete from work_authors
		where work_id = @id;

		insert into work_authors (work_id, author_id)
		select @id, json_extract(value, '$.author.key')
		from json_each (@authors);
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

		result, err := statement.ExecContext(
			ctx,
			sql.Named("id", work.Key),
			sql.Named("created", work.Created.Value),
			sql.Named("modified", work.Modified.Value),
			sql.Named("revision", work.Revision),
			sql.Named("title", work.Title),
			sql.Named("covers", covers),
			sql.Named("authors", authors),
		)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}

	return insert, statement.Close, nil
}
