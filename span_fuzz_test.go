package klogga

import (
	"context"
	"testing"
)

func FuzzTags(f *testing.F) {
	f.Add("t1", "danila1")
	f.Add("t2", "danila2")
	f.Fuzz(func(t *testing.T, tag, tagValue string) {
		spanStr := StartLeaf(context.Background()).Tag(tag, tagValue).
			Stringify()
		if spanStr == "" {
			t.Errorf("tag=%s tagValue=%s", tag, tagValue)
		}
	})
}
