package testdata

import (
	"context"
	"github.com/KasperskyLab/klogga"
	loggers5 "github.com/KasperskyLab/klogga/testdata/log.std.out.v5"
	"github.com/KasperskyLab/klogga/testdata/loggers"
	loggers4 "github.com/KasperskyLab/klogga/testdata/loggers.v4"
	loggers401 "github.com/KasperskyLab/klogga/testdata/loggers.v4.0.1"
	loggers222 "github.com/KasperskyLab/klogga/testdata/loggers/2.2.2"
	loggers2 "github.com/KasperskyLab/klogga/testdata/loggers/v2"
	loggers21 "github.com/KasperskyLab/klogga/testdata/loggers/v2.1"
	loggersv3 "github.com/KasperskyLab/klogga/testdata/loggersv3"
	"github.com/KasperskyLab/klogga/testdata/nested"
	"github.com/KasperskyLab/klogga/testdata/nested/very"
	"github.com/KasperskyLab/klogga/testdata/nested/very/deep"
	"github.com/KasperskyLab/klogga/testdata/nested/very/deep/v4/black"
	"github.com/KasperskyLab/klogga/testdata/nested/very/deep/v4/black/hole"
	loggersv6 "github.com/KasperskyLab/klogga/testdata/nested/very/deep/v4/black/hole/v6/loggers"
	loggersv7 "github.com/KasperskyLab/klogga/testdata/nested/very/deep/v4/black/hole/v6/loggers/v7"
	"github.com/KasperskyLab/klogga/testdata/parent/child"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVersionPackages(t *testing.T) {
	type iLogger interface {
		Log(trsFactory klogga.TracerProvider)
		Log2(trsFactory klogga.TracerProvider)
		Message(trsFactory klogga.TracerProvider)
	}
	for _, ts := range []struct {
		logger iLogger
		expect string
	}{
		{logger: new(loggers.Logger), expect: "loggers"},
		{logger: new(loggers2.Logger), expect: "loggers"},
		{logger: new(loggers4.Logger), expect: "loggers"},
		{logger: new(loggers401.Logger), expect: "loggers"},
		{logger: new(loggers222.Logger), expect: "loggers"},
		{logger: new(loggers21.Logger), expect: "loggers"},
		{logger: new(loggers5.Logger), expect: "log.std.out"},
		{logger: new(loggersv3.Logger), expect: "loggersv3"},
		{logger: new(loggersv6.Logger), expect: "loggers"},
		{logger: new(loggersv7.Logger), expect: "loggers"},
		{logger: new(nested.Logger), expect: "nested"},
		{logger: new(very.Logger), expect: "very"},
		{logger: new(deep.Logger), expect: "deep"},
		{logger: new(black.Logger), expect: "black"},
		{logger: new(hole.Logger), expect: "hole"},
		{logger: new(child.Logger), expect: "parent"},
	} {
		tf := klogga.NewFactory(newExporter(t, ts.expect))
		ts.logger.Log(tf)
		ts.logger.Log2(tf)
		ts.logger.Message(tf)
	}
}

type exporter struct {
	t               *testing.T
	expectedPackage string
}

func newExporter(t *testing.T, expectedPackage string) *exporter {
	t.Helper()
	return &exporter{t: t, expectedPackage: expectedPackage}
}

func (e exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	for _, span := range spans {
		require.Equal(e.t, e.expectedPackage, span.Package())
	}
	return nil
}

func (e exporter) Shutdown(context.Context) error {
	return nil
}
