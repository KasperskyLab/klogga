package examples

import (
	"context"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"klogga"
	"klogga/exporters/golog"
	"strings"
	"testing"
)

type componentExample struct {
	trs klogga.Tracer
}

func NewComponentExample(tf klogga.TracerProvider) *componentExample {
	return &componentExample{
		trs: tf.Named("example"),
	}
}

func (c componentExample) AddSuffix(ctx context.Context, input string) (string, error) {
	span, ctx := klogga.Start(ctx)
	defer c.trs.Finish(span)
	span.Val("input", input)
	if len(input) < 4 {
		return "", span.Err(errors.New("too short!"))
	}
	res := input + "=lalala"
	span.Val("res", res)
	return res, nil
}

func TestComponentLog(t *testing.T) {
	exporter := golog.New(nil)
	sb := strings.Builder{}
	tf := klogga.NewFactory(exporter, klogga.NewWriterExporter(&sb))
	ee := NewComponentExample(tf)
	suffix, err := ee.AddSuffix(context.Background(), "test_test")
	require.NoError(t, err)
	require.NotEmpty(t, suffix)

	res := sb.String()
	require.Contains(t, res, "example")
	require.Contains(t, res, "[examples.componentExample]")
	require.Contains(t, res, "I example [examples.componentExample] AddSuffix()")
	require.Contains(t, res, "test_test")
}
