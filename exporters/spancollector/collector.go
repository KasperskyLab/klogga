package spancollector

import (
	"context"
	"klogga"
)

// SpanCollector just collects written spans to public array for inspection
// currently for tests
type SpanCollector struct {
	Spans []*klogga.Span
}

func (s *SpanCollector) Write(_ context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		span.Val("span_index", len(s.Spans))
		s.Spans = append(s.Spans, span)
	}
	return nil
}

func (s *SpanCollector) Shutdown(context.Context) error {
	return nil
}
