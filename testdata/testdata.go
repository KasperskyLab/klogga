package testdata

import (
	"context"
	"github.com/pkg/errors"
	"klogga"
)

func DataBasicSpan() *klogga.Span {
	span := klogga.StartLeaf(context.Background())
	return span
}

func DataStdSpans() []*klogga.Span {
	return []*klogga.Span{
		klogga.StartLeaf(context.Background()).
			Tag("some_tag", "a").
			Val("str_val", "string lala lala").
			Val("num_val", 444),
		klogga.StartLeaf(context.Background()).
			Tag("some_tag", "a").
			Val(
				"str_lines_val", `val line1
val line2`,
			).
			Val("num_val", 444),
		klogga.StartLeaf(context.Background()).
			Tag("some_tag", "a").
			Val("str_val", "string lala lala").
			Val("num_val", 444).
			ErrSpan(errors.New("some error lalala")),
	}
}
