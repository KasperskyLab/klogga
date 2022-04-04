package batcher

import (
	"context"
	"go.kl/klogga"
	"go.kl/klogga/util/errs"
	"sync"
	"time"
)

type batcher struct {
	exporter klogga.Exporter
	conf     Config

	spans chan *klogga.Span

	cond *sync.Cond

	tm   *time.Timer
	stop chan struct{}
}

type Config struct {
	BatchSize  int
	BufferSize int // how many spans to be buffered before blocking, BatchSize*5 if zero
	Timeout    time.Duration
}

func (c *Config) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return 50
	}
	return c.BatchSize
}

func (c *Config) GetBufferSize() int {
	if c.BufferSize > 0 {
		return c.BufferSize
	}
	return c.GetBatchSize() * 5
}

func ConfigDefault() Config {
	return Config{
		BatchSize: 20,
		Timeout:   2 * time.Second,
	}
}

func New(exporter klogga.Exporter, conf Config) *batcher {
	b := &batcher{
		exporter: exporter,
		conf:     conf,
		spans:    make(chan *klogga.Span, conf.GetBufferSize()),
		cond:     sync.NewCond(&sync.Mutex{}),
		stop:     make(chan struct{}),
	}
	go b.start()

	return b
}

func (b *batcher) start() {
	b.tm = time.AfterFunc(
		b.conf.Timeout, func() {
			b.cond.Signal()
		},
	)
	go func() {
		toFlush := make([]*klogga.Span, 0, b.conf.BatchSize)
		for {
		loop:
			for len(b.spans) > 0 {
				select {
				case span := <-b.spans:
					toFlush = append(toFlush, span)
					if len(b.spans) == 0 || len(toFlush) >= b.conf.BatchSize {
						_ = b.exporter.Write(context.Background(), toFlush)
						toFlush = toFlush[:0]
						continue
					}
				default:
					break loop
				}
			}
			select {
			case <-b.stop:
				return
			default:
			}

			b.tm.Reset(b.conf.Timeout)
			b.cond.L.Lock()
			b.cond.Wait()
			b.cond.L.Unlock()
		}
	}()
}

func (b *batcher) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		select {
		case b.spans <- span:
			if len(b.spans) >= b.conf.GetBatchSize() {
				b.cond.Signal()
			}
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (b *batcher) Shutdown(ctx context.Context) (err error) {
	b.TriggerFlush()
	select {
	case b.stop <- struct{}{}:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return errs.Append(b.exporter.Shutdown(ctx), err)
}

// TriggerFlush asynchronously writes queue content to writer
func (b *batcher) TriggerFlush() {
	b.cond.Signal()
}
