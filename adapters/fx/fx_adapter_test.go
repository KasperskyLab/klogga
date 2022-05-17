package fx

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/exporters/spancollector"
	"github.com/KasperskyLab/klogga/util/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"testing"
)

func TestFxAdapter(t *testing.T) {
	collector := spancollector.SpanCollector{}
	tf := klogga.NewFactory(&collector, golog.New(nil))
	app := fx.New(
		Module(tf),
		fx.Invoke(RunTestInvoke),
	)
	err := app.Start(testutil.Timeout())
	require.NoError(t, err)
	err = app.Stop(testutil.Timeout())
	require.NoError(t, err)

	require.Contains(t, collector.Spans[3].Stringify(), "LoggerInitialized")
	require.Contains(t, collector.Spans[4].Stringify(), "RunTestInvoke")
	require.Contains(t, collector.Spans[5].Stringify(), "RunTestInvoke")
	require.Contains(t, collector.Spans[6].Stringify(), "StartSomething")

	require.Contains(t, collector.Spans[9].Stringify(), "StopSomething")
}

func RunTestInvoke(lf fx.Lifecycle) {
	lf.Append(
		fx.Hook{
			OnStart: StartSomething,
			OnStop:  StopSomething,
		},
	)
}

func StartSomething(context.Context) error { return nil }
func StopSomething(context.Context) error  { return nil }

func TestFullModule(t *testing.T) {
	app := fx.New(
		Full(),
		fx.Invoke(
			func(tf *klogga.Factory) {

			},
		),
	)
	err := app.Start(testutil.Timeout())
	require.NoError(t, err)
	err = app.Stop(testutil.Timeout())
	require.NoError(t, err)
}

type testRunner struct {
	Started, Stopped int
}

func (t *testRunner) Start(context.Context) error {
	t.Started++
	return nil
}

func (t *testRunner) Stop(context.Context) error {
	t.Stopped++
	return nil
}

func TestRunnersGroup(t *testing.T) {

	tr := &testRunner{}

	app := fxtest.New(
		t,
		fx.Provide(fx.Annotate(func() *testRunner { return tr }, TagRunner...)),
		fx.Invoke(RegisterRunners),
	)
	app.RequireStart().RequireStop()
	require.Equal(t, 1, tr.Started)
	require.Equal(t, 1, tr.Stopped)
}
