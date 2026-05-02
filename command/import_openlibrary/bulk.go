package import_openlibrary

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"kirjasto/tracing"
	"strings"
)

func insertBulk(ctx context.Context, tx *sql.Tx, table string, file io.Reader, unmarshal func(b []byte) (string, []byte, error)) error {
	ctx, span := tr.Start(ctx, "insert_bulk")
	defer span.End()

	if _, err := tx.ExecContext(ctx, "delete from "+table); err != nil {
		return tracing.Error(span, err)
	}

	batchSize := 50
	bulkInsert := strings.TrimSuffix("insert into "+table+" (id, data) values"+strings.Repeat(" (?, ?),", batchSize), ",")
	statement, err := tx.PrepareContext(ctx, bulkInsert)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer statement.Close()

	fmt.Println("Populating", table, "...")

	count := int64(0)
	values := make([]any, 0, batchSize*2)
	for data, err := range iterateFile(file) {
		count++
		if err != nil {
			return tracing.Error(span, err)
		}

		key, record, err := unmarshal(data)
		if err != nil {
			return tracing.Error(span, err)
		}

		values = append(values, key, record)

		if count%int64(batchSize) == 0 {
			_, err := statement.ExecContext(ctx, values...)
			if err != nil {
				return tracing.Error(span, err)
			}
			values = make([]any, 0, batchSize*2)
		}

		if count%10000 == 0 {
			fmt.Print(".")
		}

	}

	remainderInsert := strings.TrimSuffix("insert into "+table+" (id, data) values"+strings.Repeat(" (?, ?),", len(values)/2), ",")

	remainder, err := tx.PrepareContext(ctx, remainderInsert)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer statement.Close()

	if _, err := remainder.ExecContext(ctx, values...); err != nil {
		return tracing.Error(span, err)
	}
	fmt.Println()

	return nil
}
