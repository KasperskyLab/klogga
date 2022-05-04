package klogga

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"klogga/util/testutil"
	"testing"
)

func TestSlice_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	trs1 := NewMockExporter(ctrl)
	trs2 := NewMockExporter(ctrl)

	trs := ExportersSlice{trs1, trs2}

	span := StartLeaf(context.Background())

	slice := []*Span{span}
	trs1.EXPECT().Write(gomock.Any(), slice)
	trs2.EXPECT().Write(gomock.Any(), slice)

	err := trs.Write(testutil.Timeout(), slice)
	require.NoError(t, err)

	trs1.EXPECT().Shutdown(gomock.Any())
	trs2.EXPECT().Shutdown(gomock.Any())
	err = trs.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestSetComponent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrs := NewMockExporter(ctrl)

	component := ComponentName("test_component")

	tf := NewFactory(mockTrs)
	defer func() {
		err := tf.Shutdown(testutil.Timeout())
		require.NoError(t, err)
	}()
	trs := tf.Named(component)
	require.Equal(t, component, trs.Name())

	span := StartLeaf(context.Background()).Tag("tag_t", "tt")

	mockTrs.EXPECT().Write(gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, s []*Span) {
			require.Equal(t, component, s[0].Component())
		},
	)
	mockTrs.EXPECT().Shutdown(gomock.Any())
	span.FlushTo(trs)
}

func TestExportersSlice_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exporter := NewMockExporter(ctrl)
	exporter.EXPECT().Write(gomock.Any(), gomock.Any()).Return(errors.New("failed to write"))

	err := ExportersSlice{exporter}.Write(testutil.Timeout(), SpanSlice{})
	require.Error(t, err)
}
