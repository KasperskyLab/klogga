package temporal

import (
	"context"
	"fmt"
	"klogga"
)

// AdapterRawMessage basic adapter of temporal logger to spans
// treats keyvals as default logger does i.e. as a Sprint parameters
// therefore, no structure is assumed
// implements
// type Logger interface {
//	 Debug(msg string, keyvals ...interface{})
//	 Info(msg string, keyvals ...interface{})
//	 Warn(msg string, keyvals ...interface{})
//	 Error(msg string, keyvals ...interface{})
// }
// without the dependency, so be warned

type AdapterRawMessage struct {
	trs klogga.Tracer
}

func NewAdapterRawMessage(trs klogga.TracerProvider) *AdapterSplitter {
	return &AdapterSplitter{trs: trs.Named("temporal")}
}

func (t *AdapterRawMessage) trace(level klogga.LogLevel, msg string, keyvals ...interface{}) {
	span := klogga.StartLeaf(context.Background()).Level(level)
	defer t.trs.Finish(span)
	span.Message(fmt.Sprint(append([]interface{}{level, msg}, keyvals...)))
}

func (t *AdapterRawMessage) Debug(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t *AdapterRawMessage) Info(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t *AdapterRawMessage) Warn(msg string, keyvals ...interface{}) {
	t.trace(klogga.Warn, msg, keyvals...)
}

func (t *AdapterRawMessage) Error(msg string, keyvals ...interface{}) {
	t.trace(klogga.Error, msg, keyvals...)
}
