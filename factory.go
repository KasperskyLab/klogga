package klogga

import (
	"context"
	"github.com/KasperskyLab/klogga/util/errs"
	"github.com/KasperskyLab/klogga/util/reflectutil"
)

// Factory combines different exporters
// constructs tracers with proper names
type Factory struct {
	// exporters where all spans are sent
	exporters ExportersSlice

	// any errors are sent here
	// TBD
	// errExporter Exporter
}

// TracerProvider use to allow components/adapters to have name overrides
// implemented by Factory
type TracerProvider interface {
	NamedPkg() Tracer
	Named(componentName ComponentName) Tracer
}

func NewFactory(exporters ...Exporter) *Factory {
	return &Factory{exporters: exporters}
}

// Named creates a named tracer for specified component
func (tf *Factory) Named(componentName ComponentName) Tracer {
	return &tracerImpl{componentName: componentName, tf: tf}
}

// NamedPkg creates a named tracer with the name as package name, where this constructor is called
func (tf *Factory) NamedPkg() Tracer {
	p, _, _ := reflectutil.GetPackageClassFunc(2)
	return tf.Named(ComponentName(p))
}

func (tf *Factory) Shutdown(ctx context.Context) error {
	return tf.exporters.Shutdown(ctx)
}

// AddExporter adds another exporter to the factory.
// All  previously created tracers as well as new tracers will write to all exporters.
// Do not use for concurrently executing goroutines that write spans.
// Intended to be used in the sequential app initialization.
func (tf *Factory) AddExporter(exporter Exporter) *Factory {
	tf.exporters = append(tf.exporters, exporter)
	return tf
}

// RemoveAllExporters clear exporters list, nothing will be exported
func (tf *Factory) RemoveAllExporters() *Factory {
	tf.exporters = []Exporter{}
	return tf
}

func (tf *Factory) write(ctx context.Context, spans []*Span) error {
	return tf.exporters.Write(ctx, spans)
}

// tracer has a fixed component it writes to
// component is reset for the span on the Finish call
type tracerImpl struct {
	componentName ComponentName
	tf            *Factory
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

	// tracer shouldn't handle write errors
	// exporters should deal with them their own way
	_ = t.tf.write(context.Background(), []*Span{span})
}

type ComponentName string

func (c ComponentName) String() string {
	return string(c)
}

type ExportersSlice []Exporter

func (t ExportersSlice) Write(ctx context.Context, spans []*Span) error {
	var childErrs error
	for _, child := range t {
		err := child.Write(ctx, spans)
		childErrs = errs.Append(childErrs, err)
	}
	return childErrs
}

func (t ExportersSlice) Shutdown(ctx context.Context) error {
	var allErrs error
	for _, child := range t {
		allErrs = errs.Append(allErrs, child.Shutdown(ctx))
	}
	return allErrs
}
