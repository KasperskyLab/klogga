package influx_exporter

import (
	"context"
	"github.com/golang/mock/gomock"
	client "github.com/influxdata/influxdb1-client"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.kl/klogga"
	"go.kl/klogga/util/testutil"
	"math"
	"testing"
)

func TestBasicMarshalling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	influxClient := NewMockInfluxClient(ctrl)
	influxClient.EXPECT().Write(gomock.Any()).Do(
		func(bp client.BatchPoints) {
			for _, p := range bp.Points {
				marshalString := p.MarshalString()
				t.Log(marshalString)
				require.Contains(t, marshalString, "func=TestBasicMarshalling")
				require.Contains(t, marshalString, "dan_val=444i")
				require.Contains(t, marshalString, "val_uint=12345i")
				require.Contains(t, marshalString, "val_true=true")
				require.Contains(t, marshalString, "val_false=false")
			}
		},
	)

	span := klogga.StartLeaf(context.Background()).
		Tag("danila", "a").
		Val("dan_val", 444).
		Val("val_uint", uint64(12345)).
		Val("val_true", true).
		Val("val_false", false).
		Val(
			"val_multiline", `va line1
va line2`,
		).
		ErrSpan(errors.New("test error"))

	trs := New(&Conf{}, influxClient, klogga.NewTestErrTracker(t, klogga.NilExporterTracer{}))

	trs.Write(testutil.Timeout(), []*klogga.Span{span})
}

func TestUnsupportedBigUnsigned(t *testing.T) {
	var big uint64 = 1<<63 + 42141

	bigAdjusted := AdjustValType(big)
	require.Equal(t, -math.MaxInt64, bigAdjusted)
}
