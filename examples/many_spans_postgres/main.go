package main

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/batcher"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/exporters/postgres"
	"github.com/KasperskyLab/klogga/exporters/postgres/pgconnector"
	"go.uber.org/fx"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"
)

func main() {
	fx.New(createApp()).Run()
}

func createApp() fx.Option {
	return fx.Options(
		fx.Provide(
			func() (klogga.TracerProvider, error) {
				// this should be called explicitly,
				// so klogga doesn't have to have its own init() method
				klogga.InitHostname()

				conn := pgconnector.PgConnector{ConnectionString: os.Getenv("KLOGGA_PG_CONNECTION_STRING")}

				err := conn.CreateSchemaIfNotExists(context.Background(), postgres.DefaultSchema)
				if err != nil {
					return nil, err
				}
				pgBatcher := batcher.New(postgres.New(&postgres.Conf{}, &conn, nil), batcher.ConfigDefault())
				return klogga.NewFactory(pgBatcher, golog.New(nil)), nil
			},
			NewRunner,
		),
		fx.Invoke(
			func(tf klogga.TracerProvider) {
				trs := tf.NamedPkg()
				klogga.StartLeaf(context.Background()).
					Tag("addr", "localhost:8080").FlushTo(trs)
				go func() {
					err := http.ListenAndServe("localhost:8080", nil)
					if err != nil {
						klogga.StartLeaf(context.Background()).ErrSpan(err).FlushTo(trs)
					}
				}()
			},
			func(r *Runner, lf fx.Lifecycle) {
				lf.Append(
					fx.Hook{
						OnStart: r.Start,
						OnStop:  r.Stop,
					},
				)
			},
		),
	)
}

type Runner struct {
	trs  klogga.Tracer
	stop chan struct{}
}

func NewRunner(tf klogga.TracerProvider) *Runner {
	return &Runner{
		trs:  tf.NamedPkg(),
		stop: make(chan struct{}),
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
			case <-time.After(1 * time.Millisecond):
			}
		}
	}()
	return nil
}

func (r *Runner) Stop(ctx context.Context) error {
	span := klogga.StartLeaf(context.Background())
	defer r.trs.Finish(span)
	select {
	case r.stop <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) LogSomething(iteration int) {
	span := klogga.StartLeaf(context.Background(), klogga.WithName("run_func"))
	defer r.trs.Finish(span)
	span.Val("iteration", iteration)
}
