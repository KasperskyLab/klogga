package klogga

import (
	"context"
	"go.kl/klogga/util/errs"
	"go.kl/klogga/util/reflectutil"
)

// Factory combines different exporters
// constructs tracers with proper names
type Factory struct {
	// exporters where all spans are sent
	exporters ExportersSlice

	// any errors are sent here
	errExporter Exporter
}

func NewFactory(exporters ...Exporter) *Factory {
	return &Factory{exporters: exporters}
}

// tracer has a fixed component it writes to
// component is reset for the span on the Finish call
type tracerImpl struct {
	componentName ComponentName
	trs           Exporter
}

// Named creates a named tracer for specified component
func (tf *Factory) Named(componentName ComponentName) Tracer {
	return &tracerImpl{componentName: componentName, trs: tf.exporters}
}

// NamedPkg creates a named tracer with the name as package name, where this constructor is called
func (tf *Factory) NamedPkg() Tracer {
	p, _, _ := reflectutil.GetPackageClassFunc()
	return &tracerImpl{
		trs:           tf.exporters,
		componentName: ComponentName(p),
	}
}

func (tf *Factory) Shutdown(ctx context.Context) error {
	return tf.exporters.Shutdown(ctx)
}

func (t *tracerImpl) Name() ComponentName {
	return t.componentName
}

func (t *tracerImpl) Finish(span *Span) {
	if t.componentName != "" {
		span.component = t.componentName
	} else {
		span.component = ComponentName(span.packageName)
	}
	span.Stop()
	_ = t.trs.Write(context.Background(), []*Span{span})
}

type ComponentName string

func (c ComponentName) String() string {
	return string(c)
}

type ExportersSlice []Exporter

func (t ExportersSlice) Write(ctx context.Context, spans []*Span) error {
	var combinedErrs error
	for _, child := range t {
		err := child.Write(ctx, spans)
		combinedErrs = errs.Append(combinedErrs, err)
	}
	return nil
}

func (t ExportersSlice) Shutdown(ctx context.Context) error {
	var allErrs error
	for _, child := range t {
		allErrs = errs.Append(allErrs, child.Shutdown(ctx))
	}
	return allErrs
}
