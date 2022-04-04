package temporal_adapter

import (
	"context"
	"fmt"
	"go.kl/klogga"
	"go.kl/klogga/util/stringutil"
)

// TemporalLoggerV2Adapter basic adapter of temporal logger to spans
type TemporalLoggerV2Adapter struct {
	trs klogga.Tracer
	ctx context.Context
}

func NewTemporalLoggerV2Adapter(trs klogga.Factory, ctx context.Context) *TemporalLoggerV2Adapter {
	return &TemporalLoggerV2Adapter{trs: trs.Named("temporal"), ctx: ctx}
}

func (t TemporalLoggerV2Adapter) trace(level klogga.LogLevel, msg string, keyVals ...interface{}) {
	span := klogga.StartLeaf(t.ctx).Level(level)
	defer t.trs.Finish(span)

	var key string
	for i, keyVal := range keyVals {
		if i%2 == 0 {
			key = fmt.Sprintf("%v", keyVal)
			key = stringutil.ToSnakeCase(key)
			continue
		}
		span.Val(key, keyVal)
	}
	span.Message(msg)
}

func (t TemporalLoggerV2Adapter) Debug(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t TemporalLoggerV2Adapter) Info(msg string, keyvals ...interface{}) {
	t.trace(klogga.Info, msg, keyvals...)
}

func (t TemporalLoggerV2Adapter) Warn(msg string, keyvals ...interface{}) {
	t.trace(klogga.Warn, msg, keyvals...)
}

func (t TemporalLoggerV2Adapter) Error(msg string, keyvals ...interface{}) {
	t.trace(klogga.Error, msg, keyvals...)
}
