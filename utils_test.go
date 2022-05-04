package klogga

import (
	"context"
	"github.com/stretchr/testify/require"
	"klogga/util/testutil"
	"strings"
	"testing"
)

func TestWriterAdapter(t *testing.T) {
	span := StartLeaf(context.Background()).Tag("data", "334")
	sb := strings.Builder{}
	rTrs := NewWriterExporter(&sb)
	defer func() { require.NoError(t, rTrs.Shutdown(testutil.Timeout())) }()
	span.FlushTo(NewFactory(rTrs).NamedPkg())
	str := sb.String()
	t.Log(str)
	require.Contains(t, str, "data")
	require.Contains(t, str, "334")
}
