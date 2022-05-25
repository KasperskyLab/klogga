package otel

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/util/errs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"time"
)

// Exporter basic exporter of klogga spans to otel tracer
type Exporter struct {
	tracer trace.Tracer
}

func New(tracer trace.Tracer) *Exporter {
	return &Exporter{tracer: tracer}
}

func (t *Exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	var spanErrs error
	for _, span := range spans {
		ts, err := trace.TraceState{}.Insert("klogga", span.ID().String())
		if err != nil {
			spanErrs = errs.Append(spanErrs, err)
			continue
		}
		ctx = withIds(ctx, span.ID(), span.TraceID())
		config := trace.SpanContextConfig{
			TraceState: ts,
			TraceFlags: trace.FlagsSampled,
		}
		if pID := span.ParentID(); !pID.IsZero() {
			config.SpanID = trace.SpanID(pID)
			config.TraceID = trace.TraceID(span.TraceID())
		}
		_, otelSpan := t.tracer.Start(
			trace.ContextWithSpanContext(ctx, trace.NewSpanContext(config)),
			span.Name(),
			trace.WithTimestamp(span.StartedTs()),
		)

		for k, v := range span.Vals() {
			otelSpan.SetAttributes(
				attribute.KeyValue{
					Key:   attribute.Key(k),
					Value: ConvertValue(v),
				},
			)
		}
		for k, v := range span.Tags() {
			otelSpan.SetAttributes(
				attribute.KeyValue{
					Key:   attribute.Key(k),
					Value: ConvertValue(v),
				},
			)
		}
		if span.HasWarn() {
			otelSpan.SetAttributes(attribute.String("warn", span.Warns().Error()))
		}
		if span.HasErr() {
			otelSpan.RecordError(span.Errs())
			otelSpan.SetStatus(codes.Error, "E")
		}

		// possibly should have something like this:
		// otelSpan.SetAttributes(attribute.String("klogga_id", span.ID().String()))
		time.Sleep(500 * time.Millisecond)
		otelSpan.End()
	}
	return nil
}

func (t *Exporter) Shutdown(context.Context) error {
	return nil
}
