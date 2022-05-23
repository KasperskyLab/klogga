package pgconnector

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/batcher"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/exporters/postgres"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConnectPg(t *testing.T) {
	// SETUP (arrange)
	pgConn := &PgConnector{
		ConnectionString: testutil.IntegrationEnv(t, "KLOGGA_PG_CONNECTION_STRING"),
	}
	rawConn, err := pgConn.GetConnectionRaw(testutil.Timeout())
	require.NoError(t, err)
	// schema needs to be created manually, to avoid klogga messing up your existing structure
	_, err = rawConn.Exec("create schema if not exists audit")
	require.NoError(t, err)

	// postgres exporter needs a separate tracer, to write its own errors
	// this tracer shod use some simpler exporter, like golog, anything closer than external database
	// be careful not to recurse that )
	errTracer := klogga.NewTestErrTracker(t, klogga.NewFactory(golog.New(nil)).NamedPkg())
	pgExporter := postgres.New(
		&postgres.Conf{},
		pgConn,
		errTracer,
	)

	// for most exporters batching is recommended
	pgBatcher := batcher.New(pgExporter, batcher.ConfigDefault())
	// factory
	tf := klogga.NewFactory(pgBatcher)

	// tracer, finally!!
	trs := tf.Named("example_test")

	// SPAN CREATION (act)
	span, _ := klogga.Start(context.Background())
	span.Tag("pg", "postgres")
	span.Val("connection_string", pgConn.ConnectionString)

	trs.Finish(span)

	err = pgBatcher.Shutdown(testutil.Timeout())
	require.NoError(t, err)

	// CHECK STUFF (assert)
	row := psql.Select("pg", "connection_string").From("audit.example_test").
		RunWith(rawConn).QueryRow()
	var col1, col2 string
	err = row.Scan(&col1, &col2)
	require.NoError(t, err)
	require.Equal(t, "postgres", col1)
	require.Equal(t, pgConn.ConnectionString, col2)
}
