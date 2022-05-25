package fx

import (
	"context"
	"fmt"
	"github.com/KasperskyLab/klogga"
	"go.uber.org/fx"
	"reflect"
)

// Runner simplifies registration of components that should start and stop
type Runner interface {
	// Start starts the runner, start process can timeout by context
	// runner is responsible for creating goroutines etc
	// should not block and exit when startup is done
	Start(ctx context.Context) error

	// Stop stops the runner, the stop process can timeout by contest
	Stop(ctx context.Context) error
}

const RunnersGroupName = "runners"

var RunnersGroupAttribute = fmt.Sprintf(`group:"%s"`, RunnersGroupName)
var ResultTagRunner = fx.ResultTags(RunnersGroupAttribute)
var TagRunner = []fx.Annotation{fx.As(new(Runner)), ResultTagRunner}

type RunnersGroup struct {
	fx.In
	Runners []Runner `group:"runners"`
}

func RegisterRunners(r RunnersGroup, lc fx.Lifecycle) {
	r.Register(lc)
}

func (rr RunnersGroup) Register(lc fx.Lifecycle) {
	for _, runner := range rr.Runners {
		lc.Append(ToHook(runner))
	}
}

// RegisterRunnersWithErrors experimental watch for runner errors via channel
func RegisterRunnersWithErrors(r RunnersGroup, tf klogga.TracerProvider, lc fx.Lifecycle, s fx.Shutdowner) {
	r.RegisterWithErrors(tf, lc, s)
}

func (rr RunnersGroup) RegisterWithErrors(tf klogga.TracerProvider, lc fx.Lifecycle, s fx.Shutdowner) {
	trs := tf.NamedPkg()
	for _, runner := range rr.Runners {
		re, ok := runner.(RunnerErr)
		if !ok {
			lc.Append(ToHook(runner))
			continue
		}
		ctx, cancelFunc := context.WithCancel(context.Background())
		lc.Append(ToHookWithCtx(runner, cancelFunc))
		go func(r Runner) {
			select {
			case err := <-re.Error():
				klogga.Message("runner error").
					Tag("runner", reflect.TypeOf(r).String()).
					ErrSpan(err).FlushTo(trs)
			case <-ctx.Done():

			}

			if err := s.Shutdown(); err != nil {
				klogga.Message("shutdowner error").
					Tag("runner", reflect.TypeOf(r).String()).
					ErrSpan(err).FlushTo(trs)

			}
		}(runner)
	}
}

func ToHook(r Runner) fx.Hook {
	return fx.Hook{
		OnStart: r.Start,
		OnStop:  r.Stop,
	}
}

func ToHookWithCtx(r Runner, onStopped context.CancelFunc) fx.Hook {
	return fx.Hook{
		OnStart: r.Start,
		OnStop: func(ctx context.Context) error {
			err := r.Stop(ctx)
			onStopped()
			return err
		},
	}
}

type RunnerErr interface {
	Error() <-chan error
}
