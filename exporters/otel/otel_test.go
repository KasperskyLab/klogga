package otel

import (
	"context"
	"fmt"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/constants/vals"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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
	tp := NewTracerProvider(trace.WithSyncer(exporter))
	defer func() { require.NoError(t, tp.Shutdown(testutil.Timeout())) }()

	otelTrs := tp.Tracer("test123")

	trs := klogga.NewFactory(New(otelTrs)).NamedPkg()
	span := klogga.StartLeaf(testutil.Timeout())
	span.Tag("key1", "test_key_value")
	value := 741275
	span.Val(vals.Count, value)
	span.Warn(errors.New("some_test_warn"))
	time.Sleep(100 * time.Millisecond)
	trs.Finish(span)

	t.Log(span.Stringify())

	otelSpanStr := sb.String()
	t.Logf(otelSpanStr)
	require.Contains(t, otelSpanStr, "test_key_value")
	require.Contains(t, otelSpanStr, fmt.Sprintf("%v", value))
	require.Contains(t, otelSpanStr, "some_test_warn")
	require.Contains(t, otelSpanStr, span.ID().String())
	require.Contains(t, otelSpanStr, fmt.Sprintf("%x", span.ID().Bytes()))
	require.Contains(t, otelSpanStr, fmt.Sprintf("%x", span.TraceID().Bytes()))
}

func TestOtelExporterWithErr(t *testing.T) {
	sb := strings.Builder{}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(&sb), stdouttrace.WithPrettyPrint())
	require.NoError(t, err)
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	defer func() {
		require.NoError(t, tp.Shutdown(testutil.Timeout()))
	}()

	trs := klogga.NewFactory(New(tp.Tracer("test123"))).NamedPkg()
	span := klogga.StartLeaf(testutil.Timeout())
	span.Tag("key1", "test_key_value")
	span.ErrVoid(errors.New("some_test_error"))
	time.Sleep(100 * time.Millisecond)
	trs.Finish(span)
	t.Log(span.Stringify())

	otelSpanStr := sb.String()
	require.Contains(t, otelSpanStr, "test_key_value")
	require.Contains(t, otelSpanStr, "some_test_error")
	t.Logf(otelSpanStr)
}

func TestOtelExporterWithParentSpan(t *testing.T) {
	sb := strings.Builder{}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(&sb), stdouttrace.WithPrettyPrint())
	require.NoError(t, err)
	tp := NewTracerProvider(trace.WithSyncer(exporter))
	defer func() { require.NoError(t, tp.Shutdown(testutil.Timeout())) }()
	otelTrs := tp.Tracer("test123")

	trs := klogga.NewFactory(New(otelTrs)).NamedPkg()
	parentSpan, ctx := klogga.Start(testutil.Timeout())

	span := klogga.StartLeaf(ctx)
	span.Tag("key1", "test_key_value")
	trs.Finish(span)

	t.Log(span.Stringify())

	otelSpanStr := sb.String()
	t.Logf(otelSpanStr)
	require.Contains(t, otelSpanStr, "test_key_value")
	require.Contains(t, otelSpanStr, fmt.Sprintf("%x", parentSpan.ID().Bytes()))
	require.Contains(t, otelSpanStr, fmt.Sprintf("%x", span.ID().Bytes()))
	require.Contains(t, otelSpanStr, fmt.Sprintf("%x", span.TraceID().Bytes()))
}

func TestIdInContext(t *testing.T) {
	spanID := klogga.NewSpanID()
	ctx := context.Background()
	ctx = withIds(ctx, spanID, klogga.NewTraceID())
	ok, _, extractedID := getIds(ctx)
	require.True(t, ok)
	require.Equal(t, spanID, extractedID)
}
