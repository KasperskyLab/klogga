package pgconnector

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"klogga"
	"klogga/constants/vals"
	"klogga/exporters/postgres"
	"klogga/util/testutil"
	"testing"
	"time"
)

func TestWriteJson(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.test_json")

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_json")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	jj := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &jj)
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

func TestReloadSchema(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.component").
		Truncate(postgres.ErrorPostgresTable)

	span1, _ := klogga.Start(testutil.Timeout())
	span1.SetComponent("component")
	span1.Tag("tag", "text_value_1")

	err := pg.Write(testutil.Timeout(), klogga.SpanSlice{span1})
	require.NoError(t, err)

	row := psql.Select("count(*)").From("audit.component").
		RunWith(conn).QueryRow()
	require.Equal(t, 1, conn.ScanInt(row))

	pgNew, _ := PgConn(t)

	span2, _ := klogga.Start(testutil.Timeout())
	span2.SetComponent("component")
	span2.Tag("tag", "text_value_2")

	err = pgNew.Write(testutil.Timeout(), klogga.SpanSlice{span2})
	require.NoError(t, err)

	row = psql.Select("count(*)").From("audit.component").
		RunWith(conn).QueryRow()
	require.Equal(t, 2, conn.ScanInt(row))
}

func TestReloadSchemaCaseMessedUp(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.my_component").
		Truncate(postgres.ErrorPostgresTable)

	span1, _ := klogga.Start(testutil.Timeout())
	span1.SetComponent("MyComponent")
	span1.Tag("tag", "text_value_1")

	err := pg.Write(testutil.Timeout(), klogga.SpanSlice{span1})
	require.NoError(t, err)

	row := psql.Select("count(*)").From("audit.my_component").
		RunWith(conn).QueryRow()
	require.Equal(t, 1, conn.ScanInt(row))

	pgNew, _ := PgConn(t)

	span2, _ := klogga.Start(testutil.Timeout())
	span2.SetComponent("MyComponent")
	span2.Tag("tag", "text_value_2")

	err = pgNew.Write(testutil.Timeout(), klogga.SpanSlice{span2})
	require.NoError(t, err)

	row = psql.Select("count(*)").From("audit.my_component").
		RunWith(conn).QueryRow()
	require.Equal(t, 2, conn.ScanInt(row))
}

func TestWriteWithoutTags(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.test_tags")

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_tags")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	jj := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &jj)
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
	conn.DropIfExists("audit.test_error")

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_error")
	span.ErrVoid(errors.New("error-test"))

	err := pg.Write(testutil.Timeout(), []*klogga.Span{span})
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
	conn.DropIfExists("audit.test_incremental")

	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_incremental")

	span.Tag("login", "danila")

	require.NoError(t, pg.Write(testutil.Timeout(), []*klogga.Span{span}))

	row := psql.Select("login").From("audit.test_incremental").
		RunWith(conn).QueryRow()
	l := ""
	require.NoError(t, row.Scan(&l))
	require.Equal(t, "danila", l)

	span = klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("test_incremental")
	span.Val(vals.Count, 22)
	err := pg.Write(testutil.Timeout(), []*klogga.Span{span})
	require.NoError(t, err)

	rows, err := psql.Select("count").From("audit.test_incremental").
		RunWith(conn).Query()
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.True(t, rows.Next())
	var c *int
	require.NoError(t, rows.Scan(&c))
	require.Equal(t, 22, *c)
}

func TestBulkInsertMultipleTables(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.component1").DropIfExists("audit.component2")

	spans := make([]*klogga.Span, 0)
	for i := 0; i < 3; i++ {
		span1, _ := klogga.Start(testutil.Timeout())
		span1.SetComponent("component1")
		span1.Tag("x", "x")
		spans = append(spans, span1)

		span2, _ := klogga.Start(testutil.Timeout())
		span2.SetComponent("component2")
		span2.Tag("x", "x")
		spans = append(spans, span2)
	}
	err := pg.Write(testutil.Timeout(), spans)
	require.NoError(t, err)

	row := psql.Select("count(*)").From("audit.component1").
		RunWith(conn).QueryRow()
	var res int
	require.NoError(t, row.Scan(&res))
	require.Equal(t, 3, res)

	row = psql.Select("count(*)").From("audit.component2").
		RunWith(conn).QueryRow()
	require.NoError(t, row.Scan(&res))
	require.Equal(t, 3, res)
}

func TestWritePgTypes(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.component")

	type T struct{ Name string }
	span, _ := klogga.Start(testutil.Timeout())
	span.SetComponent("component")
	span.
		Tag("int", 11).
		Tag("int64", int64(11)).
		Tag("bool", true).
		Tag("bytes", []byte{0x01}).
		Tag("float", 1.1).
		Tag("string", "string").
		Tag("time_time", time.Now()).
		Tag("dur", time.Second).
		ValAsObj("struct", T{Name: "xxx"})

	err := pg.Write(testutil.Timeout(), klogga.SpanSlice{span})
	require.NoError(t, err)

	row := psql.Select("dur").From("audit.component").
		RunWith(conn).QueryRow()
	var dur time.Duration
	require.NoError(t, row.Scan(&dur))
	require.Equal(t, 1*time.Second, dur)
}

func TestWritePgReservedColumnName(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.component")
	span, _ := klogga.Start(testutil.Timeout())
	span.SetComponent("component")
	span.Tag("group", "group")

	err := pg.Write(testutil.Timeout(), klogga.SpanSlice{span})
	require.NoError(t, err)

	row := psql.Select("\"group\"").From("audit.component").
		RunWith(conn).QueryRow()

	res := conn.ScanString(row)
	require.Equal(t, "group", res)

}

func TestIncompatibleSchemaInOneBatch(t *testing.T) {
	pg, conn := PgConn(t)
	conn.DropIfExists("audit.component").
		Truncate(postgres.ErrorPostgresTable)

	span1, _ := klogga.Start(testutil.Timeout())
	span1.SetComponent("component")
	span1.Tag("tag", "val")
	span2, _ := klogga.Start(testutil.Timeout())
	span2.SetComponent("component")
	span2.Tag("tag", int64(11))

	err := pg.Write(testutil.Timeout(), klogga.SpanSlice{span1, span2})
	require.NoError(t, err)

	row := psql.Select("count(*)").From("audit.component").
		RunWith(conn).QueryRow()

	require.Equal(t, 1, conn.ScanInt(row))

	row = psql.Select("count(*)").From(postgres.ErrorPostgresTable).
		RunWith(conn).QueryRow()
	require.Equal(t, 1, conn.ScanInt(row))
}
