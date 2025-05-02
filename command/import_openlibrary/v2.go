package import_openlibrary

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"
	"path"

	"github.com/spf13/pflag"
)

// var tr = otel.Tracer("command.import.openlibrary")

func NewImportV2Command() *ImportV2Command {
	return &ImportV2Command{}
}

type ImportV2Command struct {
}

func (c *ImportV2Command) Synopsis() string {
	return "import openlibrary"
}

func (c *ImportV2Command) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("import.openlibrary", pflag.ContinueOnError)
	return flags
}

func (c *ImportV2Command) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command takes exactly 1 argument: a path to import")
	}

	dir := args[0]

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := c.createTables(ctx, writer); err != nil {
		return tracing.Error(span, err)
	}

	authorsFile, err := os.Open(path.Join(dir, "ol_dump_authors_2025-02-11.txt"))
	if err != nil {
		return tracing.Error(span, err)
	}
	defer authorsFile.Close()

	if err := c.populateAuthors(ctx, writer, authorsFile); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportV2Command) createTables(ctx context.Context, writer *sql.DB) error {
	ctx, span := tr.Start(ctx, "create_tables")
	defer span.End()

	statements := []string{
		`create table if not exists editions (
			id text primary key,
			data blob
		)`,
		`create virtual table if not exists editions_fts using fts5 (
			edition_id,
			title,
			subtitle
		)`,
		`create table if not exists editions_works_link (
			edition_id text,
			title_id text,
			foreign key(edition_id) references editions(id)
		)`,
		`create table if not exists editions_isbns_link (
			edition_id text,
			isbn text,
			foreign key(edition_id) references editions(id)
		)`,
		`create table if not exists authors (
			id text primary key,
			data blob
		)`,
		`create virtual table if not exists authors_fts using fts5 (
			author_id,
			name
		)`,
	}

	for _, statement := range statements {
		if _, err := writer.ExecContext(ctx, statement); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}

func (c *ImportV2Command) populateAuthors(ctx context.Context, writer *sql.DB, authorsFile io.Reader) error {
	ctx, span := tr.Start(ctx, "populate_authors")
	defer span.End()

	tx, err := writer.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	authorsStatement := `
	insert into
		authors (id, data)
		values  (@id, @data)
	on conflict(id) do update set
		data  = excluded.data
	`

	statement, err := tx.PrepareContext(ctx, authorsStatement)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer statement.Close()

	fmt.Println("Populating authors...")

	count := int64(0)
	for author, err := range iterateFile(authorsFile) {
		if err != nil {
			return tracing.Error(span, err)
		}

		record := &Record{}
		if err := json.Unmarshal(author, record); err != nil {
			return tracing.Error(span, err)
		}

		result, err := statement.ExecContext(ctx,
			sql.Named("id", record.Key),
			sql.Named("data", author),
		)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return tracing.Error(span, err)
		}
		count += rows

		if count%10000 == 0 {
			fmt.Print(".")
		}
	}

	fmt.Println()
	fmt.Println("Creating FTS table")

	ftsStatement := `
	delete from authors_fts;

	insert into authors_fts(author_id, name)
	select id, data ->> '$.name'
	from authors
	`
	if _, err := tx.ExecContext(ctx, ftsStatement); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Committing")
	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Printf("Done")

	return nil
}
