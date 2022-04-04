package pg_conn

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.kl/klogga"
	"go.kl/klogga/constants/vals"
	"go.kl/klogga/util/testutil"
	"testing"
)

func TestWriteJson(t *testing.T) {
	pg, conn := PgConn(t)

	_, err := conn.Exec("DROP TABLE IF EXISTS audit.test_json")
	require.NoError(t, err)

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_json")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	jj := make(map[string]interface{})
	err = json.Unmarshal([]byte(value), &jj)
	require.NoError(t, err)
	span.ValAsObj("json_field", jj)
	span.Val("bytes_field", []byte("danila"))

	err = pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)
	err = pg.Shutdown(testutil.Timeout())
	require.NoError(t, err)

	rows, err := psql.Select("id", "parent", "pkg_class", "json_field", "bytes_field").
		From("audit.test_json").
		RunWith(conn).
		Query()
	require.NoError(t, err)
	hasNext := rows.Next()
	require.True(t, hasNext)
	require.NoError(t, rows.Err())
	types, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, types, 5)

	require.Equal(t, "JSONB", types[3].DatabaseTypeName())
	require.Equal(t, "BYTEA", types[4].DatabaseTypeName())
}

func TestWriteWithoutTags(t *testing.T) {
	pg, conn := PgConn(t)

	_, err := conn.Exec("DROP TABLE IF EXISTS audit.test_tags")
	require.NoError(t, err)

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_tags")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	jj := make(map[string]interface{})
	err = json.Unmarshal([]byte(value), &jj)
	require.NoError(t, err)

	err = pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)
	err = pg.Shutdown(testutil.Timeout())
	require.NoError(t, err)

	rows, err := psql.
		Select("duration").
		From("audit.test_tags").
		RunWith(conn).
		Query()
	require.NoError(t, err)

	hasNext := rows.Next()
	require.True(t, hasNext)
	require.NoError(t, rows.Err())
}

func TestWriteError(t *testing.T) {
	pg, conn := PgConn(t)

	_, err := conn.Exec("DROP TABLE IF EXISTS audit.test_error")
	require.NoError(t, err)

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_error")
	span.ErrVoid(errors.New("error-test"))

	err = pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)
	err = pg.Shutdown(testutil.Timeout())
	require.NoError(t, err)

	rows, err := psql.
		Select("duration").
		From("audit.test_error").
		RunWith(conn).
		Query()
	require.NoError(t, err)

	hasNext := rows.Next()
	require.True(t, hasNext)
	require.NoError(t, rows.Err())
}

func TestIncrementalSpan(t *testing.T) {
	pg, conn := PgConn(t)

	_, err := conn.Exec("DROP TABLE IF EXISTS audit.test_incremental")
	require.NoError(t, err)

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_incremental")

	span.Tag("login", "danila")

	err = pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)

	row := psql.Select("login").From("audit.test_incremental").
		RunWith(conn).QueryRow()
	l := ""
	err = row.Scan(&l)
	require.NoError(t, err)
	require.Equal(t, "danila", l)

	span = klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_incremental")
	span.Val(vals.Count, 22)
	err = pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)

	rows, err := psql.Select("count").From("audit.test_incremental").
		RunWith(conn).Query()
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.True(t, rows.Next())
	var c *int
	err = rows.Scan(&c)
	require.NoError(t, err)
	require.Equal(t, 22, *c)
}

//
//func TestWriteByTicker(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	cfg := testConf(tc.schema)
//	cfg.WriteBatchInterval = time.Second / 10
//	cfg.BatchSize = 10
//	trs := New(cfg, tc.cf, testutil.NewTracer())
//	trs.cfg.BatchSize = 2
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	span, _ := klogga.Start(testutil.Timeout())
//	span.SetComponent("component")
//	span.Tag("tag", "val")
//	trs.Write(span)
//
//	time.Sleep(time.Second / 5)
//
//	rows := tc.Select("component", "tag")
//	require.Len(t, rows, 1)
//
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//}
//
//func TestWriteToFinishedTracer(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	span, _ := klogga.Start(testutil.Timeout())
//	span.SetComponent("component")
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//	trs.Write(span)
//}
//
//func TestWriteValAsJson(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	type T struct{ Name string }
//	val := T{Name: "xxx"}
//	span, _ := klogga.Start(testutil.Timeout())
//	span.SetComponent("component")
//	span.ValAsJson("val", val)
//
//	trs.Write(span)
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//	rows := tc.Select("component", "val")
//	v, ok := (rows[0]["val"]).(*interface{})
//	require.True(t, ok)
//	_, ok = (*v).([]byte)
//	require.True(t, ok)
//}
//
//func TestBulkInsertMultipleTables(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	trs.cfg.BatchSize = 2
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	for i := 0; i < 3; i++ {
//		span1, _ := klogga.Start(testutil.Timeout())
//		span1.SetComponent("component1")
//		span1.Tag("x", "x")
//		trs.Write(span1)
//
//		span2, _ := klogga.Start(testutil.Timeout())
//		span2.SetComponent("component2")
//		span2.Tag("x", "x")
//		trs.Write(span2)
//	}
//
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//
//	rows := tc.Select("component1", "x")
//	require.Len(t, rows, 3)
//	rows = tc.Select("component2", "x")
//	require.Len(t, rows, 3)
//}
//
//func TestAlterTable(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	span1, _ := klogga.Start(testutil.Timeout())
//	span1.SetComponent("component")
//	span1.Tag("tag1", "val1")
//	trs.Write(span1)
//	span2, _ := klogga.Start(testutil.Timeout())
//	span2.SetComponent("component")
//	span2.Tag("tag2", "val2")
//	trs.Write(span2)
//
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//
//	rows := tc.Select("component", "tag1", "tag2")
//	require.Len(t, rows, 2)
//	require.Empty(t, rows[0]["tag2"])
//	require.Empty(t, rows[1]["tag1"])
//	require.NotEmpty(t, rows[0]["tag1"])
//	require.NotEmpty(t, rows[1]["tag2"])
//}
//
//func TestWritePgTypes(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	type T struct{ Name string }
//	span, _ := klogga.Start(testutil.Timeout())
//	span.SetComponent("component")
//	span.
//		Tag("int", 11).
//		Tag("int64", int64(11)).
//		Tag("bool", true).
//		Tag("bytes", []byte{0x01}).
//		Tag("float", 1.1).
//		Tag("string", "string").
//		Tag("time_time", time.Now()).
//		Tag("dur", time.Second).
//		Tag("struct", T{Name: "xxx"})
//
//	trs.Write(span)
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//
//	rows := tc.Select("component", "dur")
//	require.Len(t, rows, 1)
//}
//
//func TestIncompatibleSchemaInOneBatch(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	trs.cfg.BatchSize = 2
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	span1, _ := klogga.Start(testutil.Timeout())
//	span1.SetComponent("component")
//	span1.Tag("tag", "val")
//	span2, _ := klogga.Start(testutil.Timeout())
//	span2.SetComponent("component")
//	span2.Tag("tag", int64(11))
//
//	trs.Write(span1)
//	trs.Write(span2)
//
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//	rows := tc.Select("component", "tag")
//	require.Len(t, rows, 1)
//
//	rows = tc.Select(errorMetricsTableName, "error")
//	require.Len(t, rows, 1)
//}
//
//func TestIncompatibleSchemaInMultipleBatches(t *testing.T) {
//	tc := NewTestContext(t)
//	defer tc.Shutdown()
//
//	trs := New(testConf(tc.schema), tc.cf, testutil.NewTracer())
//	require.NoError(t, trs.Start(testutil.Timeout()))
//
//	span1 := klogga.StartLeaf(testutil.Timeout()).Tag("tag", "val")
//	span1.SetComponent("component")
//	span2 := klogga.StartLeaf(testutil.Timeout()).Tag("tag", int64(11))
//	span2.SetComponent("component")
//
//	trs.Write(span1)
//	trs.Write(span2)
//
//	require.NoError(t, trs.Stop(testutil.Timeout()))
//	rows := tc.Select("component", "tag")
//	require.Len(t, rows, 1)
//
//	rows = tc.Select(errorMetricsTableName, "error")
//	require.Len(t, rows, 1)
//}
