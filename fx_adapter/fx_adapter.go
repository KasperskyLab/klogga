package fx_adapter

import (
	"context"
	"go.kl/klogga"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"reflect"
	"strings"
)

const FxComponent klogga.ComponentName = "fx"

type fxTracer struct {
	trs klogga.Tracer
}

func (t fxTracer) LogEvent(event fxevent.Event) {
	span := klogga.StartLeaf(context.Background())
	defer span.FlushTo(t.trs)
	span.Tag("event", reflect.TypeOf(event).Name())
	switch e := event.(type) {
	case *fxevent.OnStartExecuting:
		span.Tag("callee", e.FunctionName)
		span.Tag("caller", e.CallerName)
	case *fxevent.OnStartExecuted:
		span.Tag("callee", e.FunctionName)
		span.Tag("caller", e.CallerName)
		if e.Err != nil {
			span.ErrVoid(e.Err)
		} else {
			span.Message("runtime:" + e.Runtime.String())
		}
	case *fxevent.OnStopExecuting:
		span.Tag("callee", e.FunctionName)
		span.Tag("caller", e.CallerName)
	case *fxevent.OnStopExecuted:
		span.Tag("callee", e.FunctionName)
		span.Tag("caller", e.CallerName)
		if e.Err != nil {
			span.ErrVoid(e.Err)
		} else {
			span.Message("runtime:" + e.Runtime.String())
		}
	case *fxevent.Supplied:
		span.Message("supplied type:" + e.TypeName).ErrVoid(e.Err)
	case *fxevent.Provided:
		span.Message("output types:" + strings.Join(e.OutputTypeNames, ",")).ErrVoid(e.Err)
	case *fxevent.Invoking:
		span.Message("function: " + e.FunctionName)
	case *fxevent.Invoked:
		if e.Err != nil {
			span.ErrSpan(e.Err).
				Message("function:" + e.FunctionName + " stack:" + e.Trace)
		}
	case *fxevent.Stopping:
		span.Message("signal " + strings.ToUpper(e.Signal.String()))
	case *fxevent.Stopped:
		span.ErrVoid(e.Err)
	case *fxevent.RollingBack:
		span.ErrVoid(e.StartErr)
	case *fxevent.RolledBack:
		span.ErrVoid(e.Err)
	case *fxevent.Started:
		span.ErrVoid(e.Err)
	case *fxevent.LoggerInitialized:
		span.Message("function:" + e.ConstructorName).ErrVoid(e.Err)
	}
}

// Module send fx logs to standard tracer
func Module(tf klogga.Factory) fx.Option {
	fxTrs := &fxTracer{trs: tf.Named(FxComponent)}
	return fx.Options(
		fx.WithLogger(
			func() (fxevent.Logger, error) {
				return fxTrs, nil
			},
		),
	)
}
