package pg_conn

import (
	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.kl/klogga"
	golog "go.kl/klogga/golog_exporter"
	"go.kl/klogga/pg_exporter"
	"go.kl/klogga/util/testutil"
	"testing"
)

var psql = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

func PgConn(t *testing.T) (*pg_exporter.Exporter, *sqlx.DB) {
	if testing.Short() {
		t.Skip("longer integration test")
	}
	pgConn := &PgConnector{
		ConnectionString: testutil.IntegrationEnv(t, "PG_CONNECTION_STRING"),
	}
	conn, err := pgConn.GetConnectionRaw(testutil.Timeout())
	require.NoError(t, err)

	_, err = conn.Exec("create schema if not exists audit")
	require.NoError(t, err)

	return pg_exporter.New(
		&pg_exporter.Conf{},
		pgConn,
		klogga.NewTestErrTracker(t, klogga.NewFactory(golog.New(nil)).NamedPkg()),
	), conn
}
