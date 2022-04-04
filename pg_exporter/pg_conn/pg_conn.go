package pg_conn

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"go.kl/klogga/pg_exporter"
)

// PgConnector the simplest PG connector implementation with sqlx
type PgConnector struct {
	ConnectionString   string
	MaxOpenConnections int
	MaxIdleConnections int
}

func (p PgConnector) GetConnection(ctx context.Context) (pg_exporter.Connection, error) {
	conn, err := sqlx.ConnectContext(ctx, "postgres", p.ConnectionString)
	if err != nil {
		return nil, errors.Wrapf(err, "pg connect failed: %s", p.ConnectionString)
	}
	conn.SetMaxOpenConns(p.MaxOpenConnections)
	conn.SetMaxIdleConns(p.MaxIdleConnections)
	return conn, nil
}

func (p PgConnector) Close(context.Context) error {
	return nil
}

func (p PgConnector) GetConnectionRaw(ctx context.Context) (*sqlx.DB, error) {
	return sqlx.ConnectContext(ctx, "postgres", p.ConnectionString)
}
