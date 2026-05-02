package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"kirjasto/tracing"
	"net/url"
	"os"
	"path"
	"runtime"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

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

	db, err := otelsql.Open("sqlite3", connectionString(dbPath))
	if err != nil {
		return nil, tracing.Error(span, err)
	}
	db.SetMaxOpenConns(1)

	if err := withPragmas(ctx, db); err != nil {
		return nil, tracing.Error(span, err)
	}

	return db, nil
}

func FastWriter(ctx context.Context, dbPath string) (*sql.DB, error) {
	ctx, span := tr.Start(ctx, "open_writer")
	defer span.End()

	dir := path.Dir(dbPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, tracing.Error(span, err)
	}

	// note: no otelsql here, as we are going to do a million or so inserts in one tx
	db, err := sql.Open("sqlite3", fastConnectionString(dbPath))
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

	// you can't set this config option any other way, and I want correct semconv attributes.
	os.Setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "database")
	db, err := otelsql.Open(
		"sqlite3",
		connectionString(dbPath),
		otelsql.WithAttributesGetter(addQueryParams),
	)

	if err != nil {
		return nil, tracing.Error(span, err)
	}
	db.SetMaxOpenConns(max(4, runtime.NumCPU()))

	if err := withPragmas(ctx, db); err != nil {
		return nil, tracing.Error(span, err)
	}

	return db, nil
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

func fastConnectionString(filepath string) string {

	conn := url.Values{}
	// conn.Add("_txlock", "immediate")
	conn.Add("_journal_mode", "OFF")
	conn.Add("_busy_timeout", "5000")
	conn.Add("_synchronous", "0")
	conn.Add("_cache_size", "1000000000")
	conn.Add("_locking_mode", "EXCLUSIVE")
	// conn.Add("_foreign_keys", "true")

	return "file:" + filepath + "?" + conn.Encode()
}

func withPragmas(ctx context.Context, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, `PRAGMA temp_store = memory`); err != nil {
		return err
	}

	return nil
}

func addQueryParams(ctx context.Context, method otelsql.Method, query string, args []driver.NamedValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, len(args))
	for i, arg := range args {
		attrs[i] = attribute.String("db.query.parameter."+arg.Name, fmt.Sprint(arg.Value))
	}
	return attrs
}
