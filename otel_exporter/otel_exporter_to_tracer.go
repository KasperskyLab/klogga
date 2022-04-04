package otel_exporter

import (
	"context"
	"go.kl/klogga"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ExporterTracer basic exporter of spans to otel tracer
type ExporterTracer struct {
	tracer trace.Tracer
}

func NewExporterTracer(tracer trace.Tracer) *ExporterTracer {
	return &ExporterTracer{tracer: tracer}
}

func (t *ExporterTracer) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		_, otelSpan := t.tracer.Start(
			ctx, span.Name(),
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
		otelSpan.End()
	}
	return nil
}

func (t *ExporterTracer) Shutdown(context.Context) error {
	return nil
}
