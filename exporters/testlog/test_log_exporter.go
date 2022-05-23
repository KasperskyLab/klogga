package testlog

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"testing"
)

type Exporter struct {
	t *testing.T
}

func NewTestLogExporter(t *testing.T) *Exporter {
	t.Helper()
	return &Exporter{t: t}
}

func (e Exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		e.t.Logf(span.Stringify())
	}
	return nil
}

func (e Exporter) Shutdown(context.Context) error {
	return nil
}
