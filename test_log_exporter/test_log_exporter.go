package test_log_exporter

import (
	"context"
	"go.kl/klogga"
	"testing"
)

type TestLogExporter struct {
	t *testing.T
}

func NewTestLogExporter(t *testing.T) *TestLogExporter {
	return &TestLogExporter{t: t}
}

func (e TestLogExporter) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		e.t.Logf(span.Stringify())
	}
	return nil
}

func (e TestLogExporter) Shutdown(context.Context) error {
	return nil
}
