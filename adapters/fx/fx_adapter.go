package fx

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"reflect"
	"strings"
)

const Component klogga.ComponentName = "fx"

type fxTracer struct {
	trs klogga.Tracer
}

func (t fxTracer) LogEvent(event fxevent.Event) {
	span := klogga.StartLeaf(context.Background())
	defer span.FlushTo(t.trs)
	span.Tag("event", reflect.TypeOf(event).String())
	switch e := event.(type) {
	case *fxevent.OnStartExecuting:
		span.OverrideName(e.CallerName).Tag("callee", e.FunctionName)
	case *fxevent.OnStartExecuted:
		span.OverrideName(e.CallerName).Tag("callee", e.FunctionName)
		if e.Err != nil {
			span.ErrVoid(e.Err)
		} else {
			span.Message("runtime:" + e.Runtime.String())
		}
	case *fxevent.OnStopExecuting:
		span.OverrideName(e.CallerName).Tag("callee", e.FunctionName)
	case *fxevent.OnStopExecuted:
		span.OverrideName(e.CallerName).Tag("callee", e.FunctionName)
		if e.Err != nil {
			span.ErrVoid(e.Err)
		} else {
			span.Message("runtime:" + e.Runtime.String())
		}
	case *fxevent.Supplied:
		span.OverrideName("Supplied").Message("supplied type:" + e.TypeName).ErrVoid(e.Err)
	case *fxevent.Provided:
		span.OverrideName("Provided").
			Message("output types:" + strings.Join(e.OutputTypeNames, ",")).ErrVoid(e.Err)
	case *fxevent.Invoking:
		span.OverrideName(e.FunctionName)
	case *fxevent.Invoked:
		span.OverrideName(e.FunctionName)
		if e.Err != nil {
			span.ErrSpan(e.Err).Message("stack:" + e.Trace)
		}
	case *fxevent.Stopping:
		span.OverrideName("Stopping").Message("signal " + strings.ToUpper(e.Signal.String()))
	case *fxevent.Stopped:
		span.OverrideName("Stopped").ErrVoid(e.Err)
	case *fxevent.RollingBack:
		span.OverrideName("RollingBack").ErrVoid(e.StartErr)
	case *fxevent.RolledBack:
		span.OverrideName("RolledBack").ErrVoid(e.Err)
	case *fxevent.Started:
		span.OverrideName("Started").ErrVoid(e.Err)
	case *fxevent.LoggerInitialized:
		span.OverrideName(e.ConstructorName).ErrVoid(e.Err)
	}
}

// Module send fx logs to standard tracer
func Module(tf klogga.TracerProvider) fx.Option {
	fxTrs := &fxTracer{trs: tf.Named(Component)}
	return fx.Options(
		fx.WithLogger(
			func() (fxevent.Logger, error) {
				return fxTrs, nil
			},
		),
	)
}

// Full Set up the default logging for the app
// registering logging and the klogga factory,
// that later can be reconfigured with more loggers
func Full() fx.Option {
	tf := klogga.NewFactory(golog.New(nil))
	return fx.Options(
		Module(tf),
		fx.Supply(tf),
		fx.Provide(func(tf *klogga.Factory) klogga.TracerProvider { return tf }),
	)
}
