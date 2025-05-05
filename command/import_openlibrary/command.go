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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tr = otel.Tracer("command.import.openlibrary")

func NewImportCommand() *ImportCommand {
	return &ImportCommand{}
}

type ImportCommand struct {
}

func (c *ImportCommand) Synopsis() string {
	return "import openlibrary"
}

func (c *ImportCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("import.openlibrary", pflag.ContinueOnError)
	return flags
}

func (c *ImportCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
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

	editionsFile, err := os.Open(path.Join(dir, "ol_dump_editions_2025-02-11.txt"))
	if err != nil {
		return tracing.Error(span, err)
	}
	defer editionsFile.Close()

	if err := c.populateEditions(ctx, writer, editionsFile); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportCommand) createTables(ctx context.Context, writer *sql.DB) error {
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
			work_id text,
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
		`create table if not exists editions_authors_link (
			edition_id text,
			author_id text,
			foreign key(edition_id) references editions(id),
			foreign key(author_id) references authors(id)
		)`,
	}

	for _, statement := range statements {
		if _, err := writer.ExecContext(ctx, statement); err != nil {
			return tracing.Error(span, err)
		}
	}

	return nil
}

func (c *ImportCommand) populateAuthors(ctx context.Context, writer *sql.DB, authorsFile io.Reader) error {
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

func (c *ImportCommand) populateEditions(ctx context.Context, writer *sql.DB, editionsFile io.Reader) error {
	ctx, span := tr.Start(ctx, "populate_editions")
	defer span.End()

	if err := c.insertEditions(ctx, writer, editionsFile); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Creating FTS table")

	ftsStatement := `
	delete from editions_fts;

	insert into editions_fts(edition_id, title, subtitle)
	select id, data ->> '$.title', data ->> '$.subtitle'
	from editions
	`
	if _, err := writer.ExecContext(ctx, ftsStatement); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Creating ISBN lookup")

	isbns := `
	delete from editions_isbns_link;

	insert into editions_isbns_link(edition_id, isbn)
	select editions.id,  isbns.value
	from editions, json_each(editions.data, '$.isbn_10') isbns
	union
	select editions.id,  isbns.value
	from editions, json_each(editions.data, '$.isbn_13') isbns
	`
	if _, err := writer.ExecContext(ctx, isbns); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Creating Works lookup")
	works := `
	insert into editions_works_link(edition_id , work_id )
	select editions.id, works.value ->> '$.key'
	from editions, json_each(editions.data, '$.works') works
	`

	if _, err := writer.ExecContext(ctx, works); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Creating Authors lookup")
	authors := `
	insert into editions_authors_link(edition_id, author_id)
	select editions.id, authors.value ->> '$.key'
	from editions, json_each(editions.data, '$.authors') authors
	`

	if _, err := writer.ExecContext(ctx, authors); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Compressing DB")
	compress := `vacuum`

	if _, err := writer.ExecContext(ctx, compress); err != nil {
		return tracing.Error(span, err)
	}

	fmt.Println("Done")

	return nil
}

func (c *ImportCommand) insertEditions(ctx context.Context, writer *sql.DB, editionsFile io.Reader) error {
	ctx, span := tr.Start(ctx, "insert_editions")
	defer span.End()

	tx, err := writer.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return tracing.Error(span, err)
	}
	defer tx.Rollback()

	editionsStatement := `
	insert into
		editions (id, data)
		values  (@id, @data)
	on conflict(id) do update set
		data  = excluded.data
	`

	statement, err := tx.PrepareContext(ctx, editionsStatement)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer statement.Close()

	fmt.Println("Populating editions...")

	count := int64(0)
	for content, err := range iterateFile(editionsFile) {
		if err != nil {
			return tracing.Error(span, err)
		}

		edition := &editionDto{}
		if err := json.Unmarshal(content, edition); err != nil {

			fixed, err := c.fixAuthors(ctx, content)
			if err != nil {
				return tracing.Error(span, err)
			}

			content = fixed
		}

		result, err := statement.ExecContext(ctx,
			sql.Named("id", edition.Key),
			sql.Named("data", content),
		)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return tracing.Error(span, err)
		}
		count += rows

		if count%5000 == 0 {
			fmt.Print(".")
		}
	}

	fmt.Println()
	fmt.Println("Committing")
	if err := tx.Commit(); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func (c *ImportCommand) fixAuthors(ctx context.Context, content []byte) ([]byte, error) {
	ctx, span := tr.Start(ctx, "fix_authors")
	defer span.End()

	// sometimes the authors is an array of ids, rather than array { key: $id }

	type dto struct {
		Key     string   `json:"key"`
		Authors []string `json:"authors"`
	}

	edition := &dto{}
	if err := json.Unmarshal(content, edition); err != nil {
		return nil, tracing.Error(span, err)
	}

	span.SetAttributes(attribute.String("edition.key", edition.Key))

	fixed := make([]recordDto, len(edition.Authors))
	for i, key := range edition.Authors {
		fixed[i] = recordDto{Key: key}
	}

	// now re-unmarshal the content into a map[string]any, so we can replace the bad authors with good authors
	record := map[string]any{}
	if err := json.Unmarshal(content, &record); err != nil {
		return nil, tracing.Error(span, err)
	}

	record["authors"] = fixed

	repaired, err := json.Marshal(record)
	if err != nil {
		return nil, tracing.Errorf(span, "error processing %s: %w", edition.Key, err)
	}

	fmt.Println(edition.Key, "authors fixed")

	return repaired, nil
}

type recordDto struct {
	Key string `json:"key"`
}
