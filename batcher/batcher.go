package batcher

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/util/errs"
	"sync"
	"sync/atomic"
	"time"
)

// Batcher collects spans to send them to exporters in batches
type Batcher struct {
	exporter klogga.Exporter
	conf     Config

	spans chan *klogga.Span

	flushedCount uint64
	erredCount   uint64
	cond         *sync.Cond

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

// ConfigDefault generates default batcher config
// non-nil optional config will be used instead
func ConfigDefault(cc ...*Config) Config {
	if len(cc) > 0 && cc[0] != nil {
		return *cc[0]
	}
	return Config{
		BatchSize:  512,
		BufferSize: 2048,
		Timeout:    5 * time.Second,
	}
}

// New constructs and starts the Batcher
// beware that errors from the exporter are ignored, so if you really need them use a decorator or something
func New(exporter klogga.Exporter, conf Config) *Batcher {
	b := &Batcher{
		exporter: exporter,
		conf:     conf,
		spans:    make(chan *klogga.Span, conf.GetBufferSize()),
		cond:     sync.NewCond(&sync.Mutex{}),
		stop:     make(chan struct{}),
	}
	go b.start()

	return b
}

func (b *Batcher) FlushedCount() (res uint64) {
	return atomic.LoadUint64(&b.flushedCount)
}

func (b *Batcher) ErredCount() (res uint64) {
	return atomic.LoadUint64(&b.erredCount)
}

func (b *Batcher) start() {
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
						err := b.exporter.Write(context.Background(), toFlush)
						if err != nil {
							atomic.AddUint64(&b.erredCount, uint64(len(toFlush)))
						} else {
							atomic.AddUint64(&b.flushedCount, uint64(len(toFlush)))
						}
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

func (b *Batcher) Write(ctx context.Context, spans []*klogga.Span) error {
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

func (b *Batcher) Shutdown(ctx context.Context) (err error) {
	b.TriggerFlush()
	if b.stop == nil {
		return nil
	}
	select {
	case b.stop <- struct{}{}:
		b.stop = nil
	case <-ctx.Done():
		err = ctx.Err()
	}
	return errs.Append(b.exporter.Shutdown(ctx), err)
}

// TriggerFlush asynchronously writes queue content to writer
func (b *Batcher) TriggerFlush() {
	b.cond.Signal()
}
