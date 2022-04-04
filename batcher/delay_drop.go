package batcher

import (
	"context"
	"go.kl/klogga"
	"go.uber.org/atomic"
	"time"
)

type DelayDrop struct {
	Delay time.Duration
	Count atomic.Int32
}

func (n *DelayDrop) Write(ctx context.Context, spans []*klogga.Span) error {
	time.Sleep(n.Delay)
	n.Count.Add(int32(len(spans)))
	return nil
}

func (n *DelayDrop) Shutdown(context.Context) error {
	return nil
}
