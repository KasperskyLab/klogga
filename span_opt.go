package klogga

import "time"

type SpanOption interface {
	apply(*Span)
}

type SpanOptionFunc func(*Span)

func (f SpanOptionFunc) apply(span *Span) { f(span) }

type withTimestampOption struct {
	ts time.Time
}

// WithTimestamp make span to have started with custom timestamp
// to create a done span, use WithDone
func WithTimestamp(ts time.Time) SpanOption {
	return &withTimestampOption{ts: ts}
}

func (o withTimestampOption) apply(span *Span) {
	span.startedTs = o.ts
}

func WithTimestampUtcNow() SpanOption {
	return (SpanOptionFunc)(func(span *Span) {
		span.startedTs = time.Now().UTC()
	})
}

func WithTimestampNow() SpanOption {
	return (SpanOptionFunc)(func(span *Span) {
		span.startedTs = time.Now()
	})
}

func WithNewSpanID() SpanOption {
	return (SpanOptionFunc)(func(span *Span) {
		span.id = NewSpanID()
	})
}

func WithHostName() SpanOption {
	return (SpanOptionFunc)(func(span *Span) {
		span.host = host
	})
}

type withNameOption struct {
	name string
}

func WithName(name string) SpanOption {
	return &withNameOption{name: name}
}

func (o withNameOption) apply(span *Span) {
	span.name = o.name
}

type withTraceIDOption struct {
	traceID TraceID
}

func WithTraceID(traceID TraceID) SpanOption {
	return &withTraceIDOption{traceID: traceID}
}

func (o withTraceIDOption) apply(span *Span) {
	span.traceID = o.traceID
}

type withParentSpanIDOption struct {
	parentSpanID SpanID
}

func WithParentSpanID(parentSpanID SpanID) SpanOption {
	return &withParentSpanIDOption{parentSpanID: parentSpanID}
}

func (o withParentSpanIDOption) apply(span *Span) {
	span.parentID = o.parentSpanID
}

type withDurationOption struct {
	ts       time.Time
	duration time.Duration
}

// WithDone make already finished span
func WithDone(ts time.Time, duration time.Duration) SpanOption {
	return &withDurationOption{ts: ts, duration: duration}
}

func (o withDurationOption) apply(span *Span) {
	span.startedTs = o.ts
	span.duration = o.duration
	span.finishedTs = o.ts.Add(o.duration)
}

type withPackageClassOption struct {
	p, c string
}

// WithPackageClass overrides reflection-retrieved package and class
func WithPackageClass(p, c string) SpanOption {
	return &withPackageClassOption{p: p, c: c}
}

func (o withPackageClassOption) apply(span *Span) {
	span.packageName = o.p
	span.className = o.c
}
