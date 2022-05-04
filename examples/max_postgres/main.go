package main

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"klogga"
	fx2 "klogga/adapters/fx"
	"klogga/batcher"
	"klogga/exporters/postgres"
	"klogga/exporters/postgres/pgconnector"
	"net/http"
	"os"
	"time"
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

				conn := pgconnector.PgConnector{ConnectionString: os.Getenv("klogga_PG_CONNECTION_STRING")}
				err := conn.CreateSchemaIfNotExists(context.Background(), postgres.DefaultSchema)
				if err != nil {
					return nil, err
				}
				pgBatcher := batcher.New(
					postgres.New(&postgres.Conf{UseTimescale: true}, &conn, nil),
					batcher.Config{BatchSize: 1000, Timeout: 500 * time.Millisecond},
				)
				return klogga.NewFactory(pgBatcher), nil
			},
			NewRunner,
		),
		fx.Invoke(
			func(tf klogga.TracerProvider) {
				trs := tf.NamedPkg()
				promAddr := ":2112"
				klogga.StartLeaf(context.Background()).
					Tag("addr", promAddr).FlushTo(trs)
				go func() {
					http.Handle("/metrics", promhttp.Handler())
					err := http.ListenAndServe(promAddr, nil)
					if err != nil {
						klogga.StartLeaf(context.Background()).ErrSpan(err).FlushTo(trs)
					}
				}()
			},
			func(r *Runner, lf fx.Lifecycle) {
				lf.Append(fx2.ToHook(r))
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
			//case <-time.After(1 * time.Millisecond):
			default:
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
