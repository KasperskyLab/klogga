package examples

import (
	"context"
	"go.kl/klogga"
	"testing"
)

type testBatcher struct {
	Batches [][]*klogga.Span
	Spans   []*klogga.Span
}

func (t *testBatcher) Write(ctx context.Context, spans []*klogga.Span) error {
	t.Batches = append(t.Batches, spans)
	t.Spans = append(t.Spans, spans...)
	return nil
}

func (t testBatcher) Shutdown(context.Context) error {
	return nil
}

func TestBatcherTree(t *testing.T) {
	// TODO write the test
	//impl := batcher.New(&testBatcher{}, nil)
	//impl.Start()

}
