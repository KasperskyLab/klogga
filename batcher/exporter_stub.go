package batcher

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"sync"
)

type exporterStub struct {
	m       sync.Mutex
	Batches [][]*klogga.Span
	spans   []*klogga.Span
}

func (t *exporterStub) Write(ctx context.Context, spans []*klogga.Span) error {
	t.m.Lock()
	defer t.m.Unlock()
	t.Batches = append(t.Batches, spans)
	t.spans = append(t.spans, spans...)
	return nil
}

func (t *exporterStub) GetSpans() []*klogga.Span {
	t.m.Lock()
	defer t.m.Unlock()
	return t.spans
}

func (t *exporterStub) Shutdown(context.Context) error {
	return nil
}
