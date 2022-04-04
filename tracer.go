//go:generate mockgen -source=tracer.go -destination=tracer_mocks.go -package=klogga

package klogga

type Tracer interface {
	Finish(span *Span)
	Name() ComponentName
}
