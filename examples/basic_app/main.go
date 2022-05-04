package main

import (
	"context"
	"klogga"
	"klogga/exporters/golog"
	"time"
)

func main() {
	// kreating a factory, with the simplest exporter
	tf := klogga.NewFactory(golog.New(nil))

	// kreating a tracer with a package name
	trs := tf.NamedPkg()

	// starting a span
	// for now, we'll ignore context
	span, _ := klogga.Start(context.Background())
	// span will be written on func exit
	defer trs.Finish(span)

	// tag - potentially indexed
	span.Tag("app", "hello-world")
	// value - for metrics, or bigger values
	span.Val("meaning", 42)
	// sleep a bit, to have us some non-zero duration
	time.Sleep(154 * time.Millisecond)

}
