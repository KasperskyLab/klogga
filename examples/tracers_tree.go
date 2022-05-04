package examples

import (
	client "github.com/influxdata/influxdb1-client"
	"klogga"
	"klogga/batcher"
	"klogga/exporters/golog"
	"klogga/exporters/influxdb18"
)

func CreateTracerTree() klogga.Exporter {
	logTrs := golog.New(nil)
	influxTrs := influxdb18.New(&influxdb18.Conf{}, &client.Client{}, &klogga.NilExporterTracer{})

	_ = klogga.NewFactory(logTrs, batcher.New(influxTrs, batcher.ConfigDefault()))

	return nil
}
