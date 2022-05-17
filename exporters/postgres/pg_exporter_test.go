package postgres

import (
	"encoding/json"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMultipleDatasets(t *testing.T) {
	span1 := klogga.StartLeaf(testutil.Timeout())
	span2 := klogga.StartLeaf(testutil.Timeout())

	span1.SetComponent("table1")
	span2.SetComponent("table2")

	span1.Tag("tag", "val")
	span2.Tag("tag", "val")

	trs := New(&Conf{}, nil, klogga.NilExporterTracer{})
	datasets, _ := trs.createRecordSets(span1, span2)
	require.Len(t, datasets, 2)
	_, found := datasets["table1"]
	require.True(t, found)
	_, found = datasets["table2"]
	require.True(t, found)
}

func TestDatasetSchema(t *testing.T) {
	s1 := klogga.StartLeaf(testutil.Timeout()).Tag("t1", "v1")
	s2 := klogga.StartLeaf(testutil.Timeout()).Tag("t1", "v1").Tag("t2", "v2")
	s1.SetComponent("postgres_test")
	s2.SetComponent("postgres_test")

	trs := New(&Conf{}, nil, klogga.NilExporterTracer{})
	datasets, _ := trs.createRecordSets(s1, s2)
	require.Len(t, datasets, 1)
	_, found := datasets["postgres_test"].Schema.Column("t1")
	require.True(t, found)
	_, found = datasets["postgres_test"].Schema.Column("t2")
	require.True(t, found)
	require.Len(t, datasets["postgres_test"].Spans, 2)
}

func TestDatasetSchemaTypeChecks(t *testing.T) {
	s1 := klogga.StartLeaf(testutil.Timeout()).Tag("tag1", "v1")
	s2 := klogga.StartLeaf(testutil.Timeout()).Tag("tag1", 1)
	s1.SetComponent("pg_test")
	s2.SetComponent("pg_test")

	trs := New(&Conf{}, nil, klogga.NilExporterTracer{})
	datasets, errSpans := trs.createRecordSets(s1, s2)
	require.Len(t, errSpans, 1)
	require.Len(t, datasets, 1)
	require.Len(t, datasets["pg_test"].Spans, 1)
}

func TestAddJsonVal(t *testing.T) {
	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("pg_test")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	jj := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &jj)
	require.NoError(t, err)
	span.ValAsObj("json_field", jj)
	pg := New(&Conf{}, nil, nil)
	datasets, errCols := pg.createRecordSets(span)
	require.Empty(t, errCols)
	require.Len(t, datasets, 1)
	require.Equal(t, 11, datasets["pg_test"].Schema.ColumnsCount())
}

func TestPgValueJsonb(t *testing.T) {
	span := klogga.StartLeaf(testutil.Timeout())
	span.SetComponent("pg_test")
	value := `{"Accept":["application/json, text/plain, */*"],"Accept-Encoding":["gzip, deflate, br"],"Accept-Language":["en-US,en;q=0.9,ru;q=0.8"],"Connection":["keep-alive"],"Content-Length":["0"],"Cookie":["s_fid=145F81CE863075C0-0F188C1885167E1E; s_cc=true; _ga=GA1.1.778486296.1606136302; tipadmin-dev=2195f6f1e24d9a3bafa3aef0f55916be"],"Ik4nnm_yfy":["0AFNS9GstJWK4gYwsSr8wxKqafIGOeljdJ75pwd46QEKBWFkbWluEIT6t4UGGgFbIhRxlsLlosMkyyWktTKm5Zu+EZCVHw=="],"Origin":["http://localhost:1234"],"Referer":["http://localhost:1234/requests/submissions"],"Sec-Ch-Ua":["\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\""],"Sec-Ch-Ua-Mobile":["?0"],"Sec-Fetch-Dest":["empty"],"Sec-Fetch-Mode":["cors"],"Sec-Fetch-Site":["same-origin"],"User-Agent":["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"]}`
	span.ValAsJson("json_field", value)
	pg := New(&Conf{}, nil, nil)
	datasets, errCols := pg.createRecordSets(span)
	require.Empty(t, errCols)
	require.Len(t, datasets, 1)
	for _, set := range datasets {
		column, ok := set.Schema.Column("json_field")
		require.True(t, ok)
		require.Equal(t, PgJsonbTypeName, column.DataType)
	}
}

func TestPgValueJsonEmpty(t *testing.T) {
	val := klogga.ValJson("")
	pgt, v := GetPgTypeVal(val)
	require.Equal(t, PgJsonbTypeName, pgt)
	require.Equal(t, "null", v)
}
