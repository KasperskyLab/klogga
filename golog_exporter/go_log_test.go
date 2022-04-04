package golog

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"go.kl/klogga"
	"go.kl/klogga/util/testutil"
	"testing"
)

func TestLogTracingOutput(t *testing.T) {
	trs := New(nil)
	span, _ := klogga.Start(context.Background())
	span.Tag("tag", "tag-val").Val("val", "val-val")
	span.ErrVoid(errors.New("err"))
	err := trs.Write(testutil.Timeout(), klogga.SpanSlice{span})
	require.NoError(t, err)
}
