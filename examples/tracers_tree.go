package examples

import (
	client "github.com/influxdata/influxdb1-client"
	"go.kl/klogga"
	"go.kl/klogga/batcher"
	"go.kl/klogga/golog_exporter"
	"go.kl/klogga/influx_exporter"
)

func CreateTracerTree() klogga.Exporter {
	logTrs := golog.New(nil)
	influxTrs := influx_exporter.New(&influx_exporter.Conf{}, &client.Client{}, &klogga.NilExporterTracer{})

	_ = klogga.NewFactory(logTrs, batcher.New(influxTrs, batcher.ConfigDefault()))

	return nil
}
