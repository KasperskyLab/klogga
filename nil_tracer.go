package klogga

import "context"

type NilExporterTracer struct{}

func (NilExporterTracer) Finish(*Span) {

}

func (NilExporterTracer) Name() ComponentName {
	return "nil_tracer"
}

func (NilExporterTracer) Write(context.Context, []*Span) error {
	return nil
}

func (NilExporterTracer) Shutdown(context.Context) error {
	return nil
}
