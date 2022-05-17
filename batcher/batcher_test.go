package batcher

import (
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBatches(t *testing.T) {
	spansCount := 30
	sps := make([]*klogga.Span, 0, spansCount)
	for i := 0; i < spansCount; i++ {
		sps = append(sps, klogga.StartLeaf(testutil.Timeout()))
	}
	exporter := &exporterStub{}
	bb := New(exporter, ConfigDefault())
	trs := klogga.NewFactory(bb).NamedPkg()
	for _, sp := range sps {
		trs.Finish(sp)
	}

	err := bb.Shutdown(testutil.Timeout())
	require.NoError(t, err)
	require.Len(t, exporter.GetSpans(), spansCount)
	require.Equal(t, uint64(spansCount), bb.FlushedCount())
}

func TestWriteAllOnClose(t *testing.T) {
	sps := make([]*klogga.Span, 0, 1000)
	for i := 0; i < cap(sps); i++ {
		sps = append(sps, klogga.StartLeaf(testutil.Timeout()))

	}
	exporter := &exporterStub{}
	bb := New(
		exporter, Config{
			BatchSize: 10,
			Timeout:   5 * time.Second,
		},
	)
	trs := klogga.NewFactory(bb).NamedPkg()
	for _, sp := range sps {
		sp.FlushTo(trs)
	}
	err := bb.Shutdown(testutil.Timeout())
	require.NoError(t, err)

	require.Len(t, exporter.GetSpans(), cap(sps))
	require.Equal(t, uint64(cap(sps)), bb.FlushedCount())
}

func TestBatchPerTimer(t *testing.T) {
	tw := &exporterStub{}

	rawTracer := New(
		tw, Config{
			BatchSize: 5,
			Timeout:   100 * time.Millisecond,
		},
	)
	trs := klogga.NewFactory(rawTracer).NamedPkg()
	for i := 0; i < 30; i++ {
		klogga.StartLeaf(testutil.Timeout()).FlushTo(trs)
	}
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 5)
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 10)
}

func TestWriteBeforeStart(t *testing.T) {
	tw := &exporterStub{}
	rawTracer := New(
		tw, Config{
			BatchSize:  5,
			BufferSize: 40,
			Timeout:    100 * time.Millisecond,
		},
	)
	trs := klogga.NewFactory(rawTracer).NamedPkg()

	for i := 0; i < 30; i++ {
		klogga.StartLeaf(testutil.Timeout()).FlushTo(trs)
	}
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 5)
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 10)
}

func BenchmarkSpansBatcher(b *testing.B) {
	rawTracer := New(
		&DelayDrop{Delay: 1 * time.Millisecond}, Config{
			BatchSize: 50,
			Timeout:   1000 * time.Millisecond,
		},
	)
	tf := klogga.NewFactory(rawTracer)
	trs := tf.NamedPkg()

	for i := 0; i < b.N; i++ {
		klogga.StartLeaf(testutil.Timeout()).FlushTo(trs)
	}

	require.NoError(b, tf.Shutdown(testutil.Timeout()))

	b.ReportAllocs()
}

func BenchmarkSpansBatcherSlow(b *testing.B) {
	rawTracer := New(
		&DelayDrop{Delay: 1 * time.Millisecond}, Config{
			BatchSize: 50,
			Timeout:   1000 * time.Millisecond,
		},
	)
	tf := klogga.NewFactory(rawTracer)
	trs := tf.NamedPkg()

	for i := 0; i < b.N; i++ {
		klogga.StartLeaf(testutil.Timeout()).FlushTo(trs)
	}

	require.NoError(b, tf.Shutdown(testutil.Timeout()))
	b.ReportAllocs()
}

func TestWriteLotsOfSpans(t *testing.T) {
	tw := &exporterStub{}
	rawTracer := New(
		tw, Config{
			BatchSize: 1000,
			Timeout:   100 * time.Millisecond,
		},
	)
	tf := klogga.NewFactory(rawTracer)
	trs := tf.NamedPkg()

	for i := 0; i < 10000; i++ {
		klogga.StartLeaf(testutil.Timeout()).FlushTo(trs)
	}
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 5)
	time.Sleep(150 * time.Millisecond)
	require.True(t, len(tw.GetSpans()) >= 10)
}
