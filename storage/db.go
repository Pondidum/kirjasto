package storage

import (
	"context"
	"database/sql"
	"kirjasto/tracing"
	"net/url"
	"os"
	"path"
	"runtime"

	"go.opentelemetry.io/otel"

	_ "github.com/mattn/go-sqlite3"
)

var tr = otel.Tracer("storage")

func Writer(ctx context.Context, dbPath string) (*sql.DB, error) {
	ctx, span := tr.Start(ctx, "open_writer")
	defer span.End()

	dir := path.Dir(dbPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, tracing.Error(span, err)
	}

	db, err := sql.Open("sqlite3", connectionString(dbPath))
	if err != nil {
		return nil, tracing.Error(span, err)
	}
	db.SetMaxOpenConns(1)

	if err := withPragmas(ctx, db); err != nil {
		return nil, tracing.Error(span, err)
	}

	return db, nil
}

func Reader(ctx context.Context, dbPath string) (*sql.DB, error) {
	ctx, span := tr.Start(ctx, "open_reader")
	defer span.End()

	db, err := sql.Open("sqlite3", connectionString(dbPath))
	if err != nil {
		return nil, tracing.Error(span, err)
	}
	db.SetMaxOpenConns(max(4, runtime.NumCPU()))

	if err := withPragmas(ctx, db); err != nil {
		return nil, tracing.Error(span, err)
	}

	return db, nil
}

func CreateTables(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx,
		`create table if not exists openlibrary (
		id text not null primary key,
		type text not null,
		revision int,
		modified text,
		data blob
	) STRICT`)

	return err
}

func connectionString(filepath string) string {

	conn := url.Values{}
	conn.Add("_txlock", "immediate")
	conn.Add("_journal_mode", "WAL")
	conn.Add("_busy_timeout", "5000")
	conn.Add("_synchronous", "NORMAL")
	conn.Add("_cache_size", "1000000000")
	conn.Add("_foreign_keys", "true")

	return "file:" + filepath + "?" + conn.Encode()
}

func withPragmas(ctx context.Context, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, `PRAGMA temp_store = memory`); err != nil {
		return err
	}

	return nil
}
