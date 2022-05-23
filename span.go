package klogga

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KasperskyLab/klogga/constants"
	"github.com/KasperskyLab/klogga/util/errs"
	"github.com/KasperskyLab/klogga/util/reflectutil"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// Span describes a structured unit of log (tracing) with an interval
// Span is a (TODO) serializable
// and independent of the way it is exported (traced) to any storage
type Span struct {
	id         SpanID
	traceID    TraceID
	startedTs  time.Time
	finishedTs time.Time
	component  ComponentName

	name        string // usually a calling func name is used
	className   string // name of the struct
	packageName string

	level LogLevel

	host string // machine name where span was created

	parent   *Span
	parentID SpanID

	duration time.Duration

	tags map[string]interface{}
	vals map[string]interface{}

	// tags that are propagated to child spans
	propagatedTags map[string]interface{}

	errs      error
	warns     error
	deferErrs error
}

// Start preferred way to start a new span, automatically sets basic span fields like class, name, host
func Start(ctx1 context.Context, opts ...SpanOption) (span *Span, ctx context.Context) {
	span = &Span{
		id:             NewSpanID(),
		host:           host,
		startedTs:      time.Now(),
		tags:           map[string]interface{}{},
		vals:           map[string]interface{}{},
		propagatedTags: map[string]interface{}{},
	}

	if p := CtxActiveSpan(ctx1); p != nil {
		span.parent = p
		span.parentID = p.id
		span.traceID = p.traceID
		for k, v := range p.propagatedTags {
			span.propagatedTags[k] = v
			span.Tag(k, v)
		}
	} else {
		span.traceID = NewTraceID()
	}

	for _, opt := range opts {
		opt.apply(span)
	}

	if span.packageName == "" || span.className == "" || span.name == "" {
		packageName, className, funcName := reflectutil.GetPackageClassFunc()
		if span.packageName == "" {
			span.packageName = packageName
		}
		if span.className == "" {
			span.className = className
		}
		if span.name == "" {
			span.name = funcName
		}
	}

	return span, context.WithValue(ctx1, activeSpanKey{}, span)
}

// StartLeaf start new span without returning resulting context i.e. no child spans possibility
func StartLeaf(ctx context.Context, opts ...SpanOption) (span *Span) {
	packageName, className, funcName := reflectutil.GetPackageClassFunc()
	span, _ = Start(ctx, append([]SpanOption{
		WithName(funcName),
		WithPackageClass(packageName, className),
	}, opts...)...)
	span.packageName = packageName
	span.className = className
	return span
}

// Message is the simplest way to start a span, in the shortest way possible
// it doesn't use context, and doesn't return one.
// It is strongly discouraged to use Message unless for testing purposes.
func Message(message string, opts ...SpanOption) *Span {
	packageName, className, funcName := reflectutil.GetPackageClassFunc()
	span, _ := Start(context.Background(), append([]SpanOption{
		WithPackageClass(packageName, className),
		WithName(funcName),
	}, opts...)...)
	span.Message(message)
	return span
}

// StartFromParentID starts new span with externally defined parent span ID
// Deprecated: use SpanOptions
func StartFromParentID(ctx context.Context, parentSpanID SpanID, traceID TraceID) (*Span, context.Context) {
	p, c, f := reflectutil.GetPackageClassFunc()
	span, ctx := Start(ctx, WithPackageClass(p, c), WithName(f))
	span.parentID = parentSpanID
	span.traceID = traceID
	return span, ctx
}

func (s *Span) ID() SpanID {
	return s.id
}

func (s *Span) TraceID() TraceID {
	return s.traceID
}

func (s *Span) Parent() *Span {
	return s.parent
}

func (s *Span) Stop() {
	// no need to sync this, as the race won't matter
	if !s.finishedTs.IsZero() {
		return
	}
	s.finishedTs = time.Now()
	s.duration = s.finishedTs.Sub(s.startedTs)
}

func (s *Span) IsFinished() bool {
	return !s.finishedTs.IsZero()
}

func (s *Span) ParentID() SpanID {
	if s == nil {
		return SpanID{}
	}
	return s.parentID
}

func (s *Span) StartedTs() time.Time {
	return s.startedTs
}

func (s *Span) Host() string {
	return s.host
}

func (s *Span) Component() ComponentName {
	return s.component
}

// SetComponent should be used only in custom and special cases
// NamedTracer should decide the component name for the span
func (s *Span) SetComponent(name ComponentName) {
	s.component = name
}

func (s *Span) Class() string {
	return s.className
}

func (s *Span) Package() string {
	return s.packageName
}

func (s *Span) PackageClass() string {
	// dot format without emptiness checks is intentional,
	// this way it is easy to distinguish functions from methods
	// and there is no confusion on whether it is a package name or a struct name
	return s.packageName + "." + s.className
}

func (s *Span) Name() string {
	return s.name
}

func (s *Span) OverrideName(newName string) *Span {
	s.name = newName
	return s
}

// Tag not thread safe
func (s *Span) Tag(key string, value interface{}) *Span {
	if key == "" {
		return s
	}
	s.tags[key] = value
	return s
}

// Val not thread safe
func (s *Span) Val(key string, value interface{}) *Span {
	if key == "" {
		return s
	}
	s.vals[key] = value
	return s
}

type ObjectVal struct {
	obj interface{}
}

// ValObject explicitly indicates that the underlying value is a nested object,
// to be stored in a complex field like jsonb
func ValObject(obj interface{}) *ObjectVal {
	return &ObjectVal{obj: obj}
}

func (o ObjectVal) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.obj)
}

func (o ObjectVal) String() string {
	bb, _ := o.MarshalJSON()
	return string(bb)
}

type StringAsJSONVal struct {
	v []byte
}

// ValJson Use to directly inform klogga that is value is a valid json
// and no conversion is needed
func ValJson(v string) *ObjectVal {
	//return &ObjectVal{obj: StringAsJSONVal{v: []byte(v)}}
	jj := json.RawMessage(v)
	if v == "" {
		jj = json.RawMessage(nil)
	}
	return &ObjectVal{obj: jj}
}

func (o StringAsJSONVal) MarshalJSON() ([]byte, error) {
	if len(o.v) == 0 {
		return []byte("{}"), nil
	}
	return o.v, nil
}

func (o StringAsJSONVal) String() string {
	return string(o.v)
}

// ValAsObj shorthand for .Val(key, klogga.ValObject(value))
func (s *Span) ValAsObj(key string, value interface{}) *Span {
	if reflectutil.IsNil(value) {
		return s
	}
	s.Val(key, ValObject(value))
	return s
}

// ValAsJson shorthand for .Val(key, klogga.ValJson(value))
func (s *Span) ValAsJson(key string, value string) *Span {
	return s.Val(key, ValJson(value))
}

// GlobalTag set the tag that is also propagated to all child spans
// not thread safe
func (s *Span) GlobalTag(key string, value interface{}) *Span {
	if value == nil {
		return s
	}
	s.Tag(key, value)
	s.propagatedTags[key] = value
	return s
}

type Enricher interface {
	Enrich(span *Span) *Span
}

func (s *Span) EnrichFrom(e Enricher) *Span {
	e.Enrich(s)
	return s
}

// Err adds error to the span, subsequent call combined errors
func (s *Span) Err(err error) error {
	if err == nil {
		return nil
	}
	if s.errs == nil {
		s.errs = err
		return err
	}
	s.errs = errs.Append(s.errs, err)
	return err
}

// ErrWrapf shorthand for errors wrap
func (s *Span) ErrWrapf(err error, format string, args ...interface{}) error {
	return s.Err(errors.Wrapf(err, format, args...))
}

// ErrVoid convenience method to Err
func (s *Span) ErrVoid(err error) {
	_ = s.Err(err)
}

// ErrRecover convenience method to be used with recover() calls
func (s *Span) ErrRecover(rec interface{}, stackBytes []byte) *Span {
	if err, ok := rec.(error); ok {
		s.ErrVoid(errors.Wrap(err, string(stackBytes)))
	} else {
		//nolint:goerr113 // special case when recovering panic with alternative type
		s.ErrVoid(errors.Wrap(fmt.Errorf("%v", rec), string(stackBytes)))
	}
	return s
}

// ErrSpan Convenience method for Err that returns the Span, for chaining
func (s *Span) ErrSpan(err error) *Span {
	_ = s.Err(err)
	return s
}

// DeferErr adds defer errors to span. Not the same as Err!
func (s *Span) DeferErr(err error) *Span {
	if err == nil {
		return s
	}
	if s.deferErrs == nil {
		s.deferErrs = err
		return s
	}
	s.deferErrs = errs.Append(s.deferErrs, err)
	return s
}

func (s *Span) Warn(err error) *Span {
	if err == nil {
		return s
	}
	if s.warns == nil {
		s.warns = err
		return s
	}
	s.warns = errs.Append(s.warns, err)
	return s
}

func (s *Span) WarnWith(err error) error {
	if err == nil {
		return nil
	}
	s.warns = errs.Append(s.warns, err)
	return err
}

// Message shorthand for generic Val("message", ... ) value
// overwrites previous message
// usage of specific tags and values is preferred!
func (s *Span) Message(message string) *Span {
	return s.Val("message", message)
}

// Level for compatibility with some logging systems
// overridden by errors and warns in the Span
func (s *Span) Level(level LogLevel) *Span {
	s.level = level
	return s
}

// Tags get a copy of span tags
func (s *Span) Tags() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range s.tags {
		result[k] = v
	}
	return result
}

// Vals get a copy of span vals
func (s *Span) Vals() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range s.vals {
		result[k] = v
	}
	return result
}

func (s *Span) Errs() error {
	return s.errs
}

func (s *Span) Warns() error {
	return s.warns
}

func (s *Span) DeferErrs() error {
	return s.deferErrs
}

// Stack get all parent spans
func (s *Span) Stack() []*Span {
	result := make([]*Span, 0)
	cur := s
	for cur != nil {
		result = append(result, cur)
		cur = cur.parent
	}

	for i := len(result)/2 - 1; i >= 0; i-- {
		opp := len(result) - 1 - i
		result[i], result[opp] = result[opp], result[i]
	}
	return result
}

// Duration returns current duration for running span
// returns total duration for stopped span
func (s *Span) Duration() time.Duration {
	if s.IsFinished() {
		return s.duration
	}
	return time.Since(s.startedTs)
}

func (s *Span) HasErr() bool {
	return s.errs != nil
}

func (s *Span) HasWarn() bool {
	return s.warns != nil
}

func (s *Span) HasDeferErr() bool {
	return s.deferErrs != nil
}

// Stringify full span data string
// to be used in text tracers
// deliberately ignores host field
func (s *Span) Stringify(endWith ...string) string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(s.startedTs.Format(TimestampLayout) + " ")

	if ew := s.EWState(); ew != "" {
		sb.WriteString(ew)
	} else {
		sb.WriteString(s.level.String())
	}

	if s.component != "" {
		sb.WriteString(" " + s.component.String())
	}

	sb.WriteString(fmt.Sprintf(" [%s.%s] %s() (%v)", s.packageName, s.className, s.name, s.Duration()))

	for k, v := range s.Tags() {
		if v == "" {
			continue
		}
		vStr := fmt.Sprintf("%v", v)
		if vStr == "" {
			continue
		}
		sb.WriteString(fmt.Sprintf("; %s:'%s'", k, vStr))
	}

	for k, v := range s.Vals() {
		if v == nil || v == "" {
			continue
		}
		vStr := fmt.Sprintf("%v", v)
		if vStr == "" {
			continue
		}
		sb.WriteString(fmt.Sprintf("; %s:'%s'", k, vStr))
	}

	if spanErrors := s.Errs(); spanErrors != nil {
		sb.WriteString(fmt.Sprintf("; E:'%v'", spanErrors))
	}

	if deferErrs := s.DeferErrs(); deferErrs != nil {
		sb.WriteString(fmt.Sprintf("; DE:'%v'", deferErrs))
	}

	if warns := s.Warns(); warns != nil {
		sb.WriteString(fmt.Sprintf("; W:'%v'", warns))
	}
	if !s.id.IsZero() {
		sb.WriteString(fmt.Sprintf("; id: %s", s.id))
	}
	if !s.parentID.IsZero() {
		sb.WriteString(fmt.Sprintf("; %s: %s", constants.ParentSpanID, s.parentID))
	}
	if !s.traceID.IsZero() {
		sb.WriteString(fmt.Sprintf("; trace: %s", s.traceID))
	}
	for _, end := range endWith {
		sb.WriteString(end)
	}
	return sb.String()
}

func (s *Span) EWState() string {
	res := ""
	if s.Errs() != nil || s.DeferErrs() != nil {
		res += "E"
	} else if s.Warns() != nil {
		res += "W"
	}
	return res
}

func (s *Span) Json() ([]byte, error) {
	if s == nil {
		return nil, nil
	}

	jsonStruct := struct {
		ID           SpanID
		ParentID     SpanID
		TraceID      TraceID
		Ts           string
		Level        string
		PackageClass string
		Name         string
		Duration     time.Duration
		Error        error
		DeferError   error
		Warn         error
		Tags         map[string]interface{}
		Vals         map[string]interface{}
	}{
		ID:           s.id,
		ParentID:     s.parentID,
		TraceID:      s.traceID,
		Ts:           s.startedTs.Format(TimestampLayout),
		Level:        s.EWState(),
		PackageClass: s.PackageClass(),
		Name:         s.name,
		Duration:     s.Duration(),
		Error:        s.Errs(),
		DeferError:   s.DeferErrs(),
		Warn:         s.Warns(),
		Tags:         s.Tags(),
		Vals:         s.Vals(),
	}
	return json.Marshal(&jsonStruct)
}

// FlushTo accept tracer and call trs.Finish, shorthand for chaining
func (s *Span) FlushTo(trs Tracer) {
	trs.Finish(s)
}

// CreateErrSpanFrom creates span describing an error in a flat way
func CreateErrSpanFrom(ctx context.Context, span *Span) *Span {
	if !span.HasErr() {
		return nil
	}

	errSpan := StartLeaf(ctx, WithTraceID(span.TraceID()))
	errSpan.parent = span
	errSpan.startedTs = span.StartedTs()
	errSpan.Tag("component", span.Component())
	errSpan.host = span.Host()
	errSpan.name = span.Name()
	errSpan.className = span.Class()
	errSpan.errs = span.errs
	errSpan.warns = span.warns
	errSpan.deferErrs = span.deferErrs

	errSpan.ValAsObj("tags", span.Tags())
	errSpan.ValAsObj("vals", span.Vals())

	return errSpan
}
