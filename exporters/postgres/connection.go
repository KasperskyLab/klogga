package postgres

import (
	"context"
	"database/sql"
)

type Connection interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

// Connector provides PG connections in an abstract way
type Connector interface {
	GetConnection(ctx context.Context) (Connection, error)
	// Close for cleanup, will be  called when tracer closes
	Close(ctx context.Context) error
}
