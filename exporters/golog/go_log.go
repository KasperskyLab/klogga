package golog

import (
	"context"
	"klogga"
	"log"
	"os"
)

type Exporter struct {
	logger *log.Logger
}

// New tracer to logger
// if logger is nil, creates a default stderr logger
// mind that date time prefix is handled by the span, not by logger
func New(logger *log.Logger) *Exporter {
	if logger == nil {
		logger = log.New(os.Stderr, "", 0)
	}
	return &Exporter{logger: logger}
}

func (t *Exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			t.logger.Println(span.Stringify())
		}
	}
	return nil
}

func (t *Exporter) Shutdown(context.Context) error {
	return nil
}
