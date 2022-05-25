package klogga

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"math"
	"testing"
	"time"
)

const TimestampLayout = "2006-01-02 15:04:05.000"

type WriterExporter struct {
	w io.Writer
}

func NewWriterExporter(writer io.Writer) *WriterExporter {
	return &WriterExporter{w: writer}
}

func (w WriterExporter) Write(ctx context.Context, spans []*Span) error {
	for _, span := range spans {
		_, _ = w.w.Write([]byte(span.Stringify("\n")))
	}
	return nil
}

func (w WriterExporter) Shutdown(context.Context) error {
	return nil
}

// WriterTracer adapts tracer to a Writer interface
type WriterTracer struct {
	trs Tracer
}

func NewWriterTracer(trs Tracer) *WriterTracer {
	return &WriterTracer{trs: trs}
}

func (t WriterTracer) Write(p []byte) (n int, err error) {
	StartLeaf(context.Background()).Message(string(p)).
		FlushTo(t.trs)
	return len(p), nil
}

// Printf for compatibility with new redis and other logging systems
func (t WriterTracer) Printf(ctx context.Context, format string, v ...interface{}) {
	StartLeaf(ctx).Message(fmt.Sprintf(format, v...)).FlushTo(t.trs)
}

// RoundDur converts duration to rounded a string value to be used in the index
func RoundDur(d time.Duration) string {
	switch {
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%vms", d.Milliseconds())
	case d < 100*time.Millisecond:
		return fmt.Sprintf("%v0ms", int(math.Round(float64(d.Milliseconds())/10)))
	case d < time.Second:
		return fmt.Sprintf("%v00ms", int(math.Round(float64(d.Milliseconds())/100)))
	case d < 10*time.Second:
		return fmt.Sprintf("%vsec", int(math.Round(d.Seconds())))
	case d < time.Minute:
		return fmt.Sprintf("%v0sec", int(math.Round(d.Seconds()/10)))
	case d < 10*time.Minute:
		return fmt.Sprintf("%vmin", int(math.Round(d.Minutes())))
	case d < time.Hour:
		return fmt.Sprintf("%v0min", int(math.Round(d.Minutes()/10)))
	default:
		return "hours"
	}
}

// NewTestErrTracker wraps tracer to fail test on the span error.
func NewTestErrTracker(t *testing.T, trs Tracer) Tracer {
	t.Helper()
	return errorTracker{
		t:   t,
		trs: trs,
	}
}

type errorTracker struct {
	t   *testing.T
	trs Tracer
}

func (e errorTracker) Name() ComponentName {
	return e.trs.Name()
}

func (e errorTracker) Finish(span *Span) {
	e.trs.Finish(span)
	if span.Errs() != nil {
		assert.Fail(e.t, "error span detected:", span.Stringify())
		return
	}
}
