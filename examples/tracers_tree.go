package examples

import (
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/batcher"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/exporters/influxdb18"
	client "github.com/influxdata/influxdb1-client"
)

func CreateTracerTree() klogga.Exporter {
	logTrs := golog.New(nil)
	influxTrs := influxdb18.New(&influxdb18.Conf{}, &client.Client{}, &klogga.NilExporterTracer{})

	_ = klogga.NewFactory(logTrs, batcher.New(influxTrs, batcher.ConfigDefault()))

	return nil
}
