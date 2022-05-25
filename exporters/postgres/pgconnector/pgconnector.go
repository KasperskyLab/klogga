package pgconnector

import (
	"context"
	"github.com/KasperskyLab/klogga/exporters/postgres"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"sync"
)

// PgConnector the simplest PG connector implementation with sqlx
// Start doesn't need to be explicitly called, but it is preferred to check connections
type PgConnector struct {
	ConnectionString   string
	MaxOpenConnections int
	MaxIdleConnections int

	connLock sync.Mutex
	conn     *ManagedSql
}

// ManagedSql wrapper that prevents users from closing the connection
type ManagedSql struct {
	*sqlx.DB
}

func (m ManagedSql) Close() error {
	return nil
}

func (p *PgConnector) Start(ctx context.Context) error {
	return p.tryInitConnection(ctx)
}

func (p *PgConnector) tryInitConnection(ctx context.Context) error {
	if p.conn != nil {
		return nil
	}
	p.connLock.Lock()
	defer p.connLock.Unlock()
	if p.conn != nil {
		return nil
	}
	conn, err := sqlx.ConnectContext(ctx, "postgres", p.ConnectionString)
	if err != nil {
		return errors.Wrapf(err, "pg connect failed: %s", p.ConnectionString)
	}
	conn.SetMaxOpenConns(p.MaxOpenConnections)
	conn.SetMaxIdleConns(p.MaxIdleConnections)
	p.conn = &ManagedSql{conn}
	return nil
}

func (p *PgConnector) GetConnection(ctx context.Context) (postgres.Connection, error) {
	if err := p.tryInitConnection(ctx); err != nil {
		return nil, err
	}
	return p.conn, nil
}

func (p *PgConnector) Stop(_ context.Context) error {
	p.connLock.Lock()
	defer p.connLock.Unlock()
	if p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

func (p *PgConnector) GetConnectionRaw(ctx context.Context) (*ManagedSql, error) {
	if err := p.tryInitConnection(ctx); err != nil {
		return nil, err
	}
	return p.conn, nil
}

// CreateSchemaIfNotExists shorthand to create schema, if you don't want to do in manually
func (p *PgConnector) CreateSchemaIfNotExists(ctx context.Context, schema string) error {
	conn, err := p.GetConnectionRaw(ctx)
	if err != nil {
		return err
	}
	_, err = conn.Exec("create schema if not exists " + schema)
	return err
}
