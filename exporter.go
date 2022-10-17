//go:generate mockgen -source=exporter.go -destination=exporter_mocks.go -package=klogga
package klogga

import (
	"context"
)

// Exporter generic tracer interface, should not be used outside implementations
// to be more generic accepts batches right away
type Exporter interface {
	Write(ctx context.Context, spans []*Span) error

	// Shutdown is called to cleanup the exporter. Exporter cannot be used after that.
	// Should be idempotent i.e. should work fine when called multiple times.
	Shutdown(ctx context.Context) error
}

// SpanSlice shorthand for exporters input
type SpanSlice []*Span
