package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"errors"

	"github.com/lib/pq"
)

const uniqueViolation = "23505"

var batchSize = 800000

// Set of error variables for CRUD operations.
var (
	ErrDBDuplicatedEntry = errors.New("duplicated entry")
)

// WithinTran runs passed function and do commit/rollback at the end.
func WithinTran(ctx context.Context, db *sql.DB, iso sql.IsolationLevel, fn func(tx *sql.Tx) error) error {
	opts := &sql.TxOptions{Isolation: iso}
	// Begin the transaction.
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tran: %w", err)
	}

	// Mark to the defer function a rollback is required.
	mustRollback := true

	// Set up a defer function for rolling back the transaction. If
	// mustRollback is true it means the call to fn failed, and we
	// need to roll back the transaction.
	defer func() {
		if mustRollback {
			if err := tx.Rollback(); err != nil {
				log.Printf("unable to rollback tran: %+v", err)
			}
		}
	}()

	// Execute the code inside the transaction. If the function
	// fails, return the error and the defer function will roll back.
	if err := fn(tx); err != nil {
		// Checks if the error is of code 23505 (unique_violation).
		if pqerr, ok := err.(*pq.Error); ok && pqerr.Code == uniqueViolation {
			return ErrDBDuplicatedEntry
		}

		return fmt.Errorf("exec tran: %w", err)
	}

	// Disarm the deferred rollback.
	mustRollback = false

	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tran: %w", err)
	}

	return nil
}

// BulkUpsert is like BulkInsert, but instead of a conflict action, a list of
// conflicting columns is provided. An "ON CONFLICT (conflict_columns) DO
// UPDATE" clause is added to the statement, with assignments "c=excluded.c" for
// every column c.
func BulkUpsert(ctx context.Context, db *sql.DB, table string, columns []string, values []interface{}, conflictColumns []string, excludeColumns []string) error {
	excludeColumnsSet := make(map[string]bool)
	for _, ec := range excludeColumns {
		excludeColumnsSet[ec] = true
	}
	conflictAction := buildUpsertConflictAction(columns, conflictColumns, excludeColumnsSet)
	return BulkInsert(ctx, db, table, columns, values, conflictAction)
}

func BulkInsert(ctx context.Context, db *sql.DB, table string, columns []string, values []interface{}, conflictAction string) (err error) {
	pageSize := (batchSize / len(columns)) * len(columns)
	total := len(values)
	offset := pageSize

	for left := 0; left < total; left += offset {
		right := left + offset
		if right > total {
			right = total
		}
		err = bulkInsert(ctx, db, table, columns, nil, values[left:right], conflictAction, nil)
	}
	return err
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, columns, returningColumns []string, values []interface{}, conflictAction string, scanFunc func(*sql.Rows) error) (err error) {
	if remainder := len(values) % len(columns); remainder != 0 {
		return fmt.Errorf("modulus of len(values) and len(columns) must be 0: got %d", remainder)
	}
	// Postgres supports up to 65535 parameters, but stop well before that
	// so we don't construct humongous queries.
	const maxParameters = 1000
	stride := (maxParameters / len(columns)) * len(columns)
	if stride == 0 {
		// This is a pathological case (len(columns) > maxParameters), but we
		// handle it cautiously.
		return fmt.Errorf("too many columns to insert: %d", len(columns))
	}

	prepare := func(n int) (*sql.Stmt, error) {
		return db.PrepareContext(ctx, buildInsertQuery(table, columns, returningColumns, n, conflictAction))
	}

	var stmt *sql.Stmt
	for leftBound := 0; leftBound < len(values); leftBound += stride {
		rightBound := leftBound + stride
		if rightBound <= len(values) && stmt == nil {
			stmt, err = prepare(stride)
			if err != nil {
				return err
			}
			defer stmt.Close()
		} else if rightBound > len(values) {
			rightBound = len(values)
			stmt, err = prepare(rightBound - leftBound)
			if err != nil {
				return err
			}
			defer stmt.Close()
		}
		valueSlice := values[leftBound:rightBound]
		var err error
		if returningColumns == nil {
			_, err = stmt.ExecContext(ctx, valueSlice...)
		} else {
			var rows *sql.Rows
			rows, err = stmt.QueryContext(ctx, valueSlice...)
			if err != nil {
				return err
			}
			_, err = processRows(rows, scanFunc)
		}
		if err != nil {
			return fmt.Errorf("running bulk insert query, values[%d:%d]): %w", leftBound, rightBound, err)
		}
	}
	return nil
}

func buildInsertQuery(table string, columns, returningColumns []string, nvalues int, conflictAction string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s", table)
	fmt.Fprintf(&b, "(%s) VALUES", strings.Join(columns, ", "))

	var placeholders []string
	for i := 1; i <= nvalues; i++ {
		// Construct the full query by adding placeholders for each
		// set of values that we want to insert.
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		if i%len(columns) != 0 {
			continue
		}

		// When the end of a set is reached, write it to the query
		// builder and reset placeholders.
		fmt.Fprintf(&b, "(%s)", strings.Join(placeholders, ", "))
		placeholders = nil

		// Do not add a comma delimiter after the last set of values.
		if i == nvalues {
			break
		}
		b.WriteString(", ")
	}
	if conflictAction != "" {
		b.WriteString(" " + conflictAction)
	}
	if len(returningColumns) > 0 {
		fmt.Fprintf(&b, " RETURNING %s", strings.Join(returningColumns, ", "))
	}
	return b.String()
}

func processRows(rows *sql.Rows, f func(*sql.Rows) error) (int, error) {
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
		if err := f(rows); err != nil {
			return n, err
		}
	}
	return n, rows.Err()
}

func buildUpsertConflictAction(columns, conflictColumns []string, excludeColumns map[string]bool) string {
	var sets []string
	for _, c := range columns {
		if _, ok := excludeColumns[c]; ok {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=excluded.%[1]s", c))
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s",
		strings.Join(conflictColumns, ", "),
		strings.Join(sets, ", "))
}
