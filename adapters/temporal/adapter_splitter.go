package temporal

import (
	"context"
	"fmt"
	"klogga"
	"klogga/util/stringutil"
)

// AdapterSplitter basic adapter of temporal logger to spans
// assumes that keyvals are (key,value) pairs to have some semblance of structure
// implements
// type Logger interface {
// 		Debug(msg string, keyvals ...interface{})
// 		Info(msg string, keyvals ...interface{})
// 		Warn(msg string, keyvals ...interface{})
// 		Error(msg string, keyvals ...interface{})
// }
// without the dependency, so be warned
type AdapterSplitter struct {
	trs klogga.Tracer
}

func NewAdapterSplitter(trs klogga.TracerProvider) *AdapterSplitter {
	return &AdapterSplitter{trs: trs.Named("temporal")}
}

func (t *AdapterSplitter) trace(level klogga.LogLevel, msg string, keyvals ...interface{}) {
	span := klogga.StartLeaf(context.Background()).Level(level)
	defer t.trs.Finish(span)

	var key string
	for i, keyVal := range keyvals {
		if i%2 == 0 {
			key = fmt.Sprintf("%v", keyVal)
			key = stringutil.ToSnakeCase(key)
			continue
		}
		span.Val(key, keyVal)
	}
	span.Message(msg)
}

func (t *AdapterSplitter) Debug(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t *AdapterSplitter) Info(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t *AdapterSplitter) Warn(msg string, keyvals ...interface{}) {
	t.trace(klogga.Warn, msg, keyvals...)
}

func (t *AdapterSplitter) Error(msg string, keyvals ...interface{}) {
	t.trace(klogga.Error, msg, keyvals...)
}
