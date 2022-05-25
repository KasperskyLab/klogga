package otel

import (
	"context"
	"github.com/KasperskyLab/klogga"
	trace_sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func NewTracerProvider(opts ...trace_sdk.TracerProviderOption) *trace_sdk.TracerProvider {
	opts = append(opts, trace_sdk.WithIDGenerator(IDImporter{}))
	return trace_sdk.NewTracerProvider(opts...)
}

// IDImporter allows OpenTelemetry spans to have same ids as klogga spans from which they were creates
type IDImporter struct {
}

type idImporterKey struct{}

type ids struct {
	spanID  klogga.SpanID
	traceID klogga.TraceID
}

func withIds(ctx context.Context, spanID klogga.SpanID, traceID klogga.TraceID) context.Context {
	return context.WithValue(
		ctx, idImporterKey{}, &ids{
			spanID:  spanID,
			traceID: traceID,
		},
	)
}

func getIds(ctx context.Context) (bool, klogga.TraceID, klogga.SpanID) {
	idsVal, ok := ctx.Value(idImporterKey{}).(*ids)
	if !ok {
		return false, klogga.TraceID{}, klogga.SpanID{}
	}
	return true, idsVal.traceID, idsVal.spanID
}

func (i IDImporter) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	ok, traceID, spanID := getIds(ctx)
	if !ok {
		return trace.TraceID(klogga.NewTraceID()), trace.SpanID(klogga.NewSpanID())
	}
	return trace.TraceID(traceID), trace.SpanID(spanID)
}

func (i IDImporter) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	ok, _, spanID := getIds(ctx)
	if !ok {
		return trace.SpanID(klogga.NewSpanID())
	}
	return trace.SpanID(spanID)
}
