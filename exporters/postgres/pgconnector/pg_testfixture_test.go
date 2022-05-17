package pgconnector

import (
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/exporters/postgres"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"testing"
)

var psql = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

type SQLTestEx struct {
	*sqlx.DB
	t *testing.T
}

func (sql SQLTestEx) DropIfExists(table string) SQLTestEx {
	_, err := sql.Exec("DROP TABLE IF EXISTS " + table)
	require.NoError(sql.t, err)
	return sql
}

func (sql SQLTestEx) Truncate(table string) SQLTestEx {
	_, _ = sql.Exec("TRUNCATE " + table)
	return sql
}

func (sql SQLTestEx) ScanInt(row squirrel.RowScanner) (res int) {
	require.NoError(sql.t, row.Scan(&res))
	return res
}

func (sql SQLTestEx) ScanString(row squirrel.RowScanner) (res string) {
	require.NoError(sql.t, row.Scan(&res))
	return res
}

func PgConn(t *testing.T) (*postgres.Exporter, SQLTestEx) {
	t.Helper()
	if testing.Short() {
		t.Skip("longer integration test")
	}
	pgConn := &PgConnector{
		ConnectionString: testutil.IntegrationEnv(t, "klogga_PG_CONNECTION_STRING"),
	}
	conn, err := pgConn.GetConnectionRaw(testutil.Timeout())
	require.NoError(t, err)

	return postgres.New(
		&postgres.Conf{},
		pgConn,
		klogga.NewTestErrTracker(t, klogga.NewFactory(golog.New(nil)).NamedPkg()),
	), SQLTestEx{conn, t}
}
