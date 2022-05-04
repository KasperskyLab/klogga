package klogga

import "context"

type activeSpanKey struct{}

// CtxActiveSpan returns current active span from context
// (for new spans, current span is a parent span)
// careful no to Write this span, parent call should do it
func CtxActiveSpan(ctx context.Context) *Span {
	if parentObj := ctx.Value(activeSpanKey{}); parentObj != nil {
		if _parent, ok := parentObj.(*Span); ok {
			return _parent
		}
	}
	return nil
}
