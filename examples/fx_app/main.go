package main

import (
	"context"
	"github.com/KasperskyLab/klogga"
	fxAdapter "github.com/KasperskyLab/klogga/adapters/fx"
	"go.uber.org/fx"
	"time"
)

func main() {
	fx.New(CreateApp()).Run()
}

func CreateApp() fx.Option {
	return fx.Options(
		fx.Provide(NewRunner,
			fx.Annotate(func(r *Runner) fxAdapter.Runner { return r }, fxAdapter.TagRunner...)),
		fxAdapter.Full(),
		fx.Invoke(fxAdapter.RegisterRunners), // register runnerA
	)
}

type Runner struct {
	trs  klogga.Tracer
	stop chan struct{}
}

func NewRunner(trsFactory klogga.TracerProvider) *Runner {
	return &Runner{
		trs: trsFactory.Named("runner_a"),
	}
}

func (r *Runner) Start(ctx context.Context) error {
	span := klogga.StartLeaf(ctx)
	defer r.trs.Finish(span)

	go func() {
		for i := 0; ; i++ {
			r.LogSomething(i)
			select {
			case <-r.stop:
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
	return nil
}

func (*Runner) Stop(context.Context) error {
	return nil
}

func (r *Runner) LogSomething(iteration int) {
	span := klogga.StartLeaf(context.Background(), klogga.WithName("run_func"))
	defer r.trs.Finish(span)
	span.Val("iteration", iteration)
}
