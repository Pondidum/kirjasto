package importcmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

func importWorkCommand(writer *sql.DB) (importWork, error) {

	works := `
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
	`

	fts := `
		insert or replace into works_fts(id, title)
		select @id, @title
		where not exists (select * from works_fts where id = @id)
	`

	clearLinks := `
		delete from work_authors
		where work_id = @id;
	`

	addLinks := `
		insert into work_authors (work_id, author_id)
		select @id, json_extract(value, '$.key')
		from json_each (@authors);
	`

	insert := func(ctx context.Context, work workDto) (int64, error) {
		covers, err := json.Marshal(work.Covers)
		if err != nil {
			return 0, err
		}

		authors, err := json.Marshal(work.Authors)
		if err != nil {
			return 0, err
		}
		fmt.Println(string(authors))

		id := sql.Named("id", work.Key)
		title := sql.Named("title", work.Title)

		result, err := writer.ExecContext(ctx, works,
			id,
			sql.Named("created", work.Created.Value),
			sql.Named("modified", work.Modified.Value),
			sql.Named("revision", work.Revision),
			title,
			covers,
		)
		if err != nil {
			return 0, err
		}

		_, err = writer.ExecContext(ctx, fts,
			id,
			title,
		)
		if err != nil {
			return 0, err
		}

		_, err = writer.ExecContext(ctx, clearLinks, id)
		if err != nil {
			return 0, err
		}

		_, err = writer.ExecContext(ctx, addLinks,
			id,
			authors,
		)
		if err != nil {
			return 0, err
		}

		return result.RowsAffected()
	}

	return insert, nil
}
