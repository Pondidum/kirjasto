package import_openlibrary

import (
	"context"
	"database/sql"
	"kirjasto/config"
	"kirjasto/storage"
	"kirjasto/tracing"

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

	// if len(args) != 1 {
	// 	return tracing.Errorf(span, "this command takes exactly 1 argument: a path to import")
	// }

	// dir := args[0]

	writer, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	if err := c.createTables(ctx, writer); err != nil {
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
