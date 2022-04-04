package otel_exporter

import (
	"github.com/stretchr/testify/require"
	"go.kl/klogga"
	"go.kl/klogga/util/testutil"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	_ "go.opentelemetry.io/otel/trace"
	"strings"
	"testing"
	"time"
)

func TestOtelExporter(t *testing.T) {
	sb := strings.Builder{}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(&sb), stdouttrace.WithPrettyPrint())
	require.NoError(t, err)
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	defer func() {
		require.NoError(t, tp.Shutdown(testutil.Timeout()))
	}()

	trs := klogga.NewFactory(NewExporterTracer(tp.Tracer("test123"))).NamedPkg()
	span := klogga.StartLeaf(testutil.Timeout())
	span.Tag("key1", "test_key_value")
	time.Sleep(400 * time.Millisecond)
	trs.Finish(span)
	t.Log(span.Stringify())

	require.Contains(t, sb.String(), "test_key_value")
	t.Logf(sb.String())

}
