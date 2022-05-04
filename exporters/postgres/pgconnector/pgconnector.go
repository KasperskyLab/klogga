package pgconnector

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"klogga/exporters/postgres"
)

// PgConnector the simplest PG connector implementation with sqlx
type PgConnector struct {
	ConnectionString   string
	MaxOpenConnections int
	MaxIdleConnections int
}

func (p *PgConnector) GetConnection(ctx context.Context) (postgres.Connection, error) {
	conn, err := sqlx.ConnectContext(ctx, "postgres", p.ConnectionString)
	if err != nil {
		return nil, errors.Wrapf(err, "pg connect failed: %s", p.ConnectionString)
	}
	conn.SetMaxOpenConns(p.MaxOpenConnections)
	conn.SetMaxIdleConns(p.MaxIdleConnections)

	return conn, nil
}

func (p *PgConnector) Close(context.Context) error {
	return nil
}

func (p *PgConnector) GetConnectionRaw(ctx context.Context) (*sqlx.DB, error) {
	return sqlx.ConnectContext(ctx, "postgres", p.ConnectionString)
}

// CreateSchemaIfNotExists shorthand to create schema, if you don't want to do in manually
func (p PgConnector) CreateSchemaIfNotExists(ctx context.Context, schema string) error {
	conn, err := p.GetConnectionRaw(ctx)
	if err != nil {
		return err
	}
	_, err = conn.Exec("create schema if not exists " + schema)
	return err
}
