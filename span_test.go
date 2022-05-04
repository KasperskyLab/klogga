package klogga

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"klogga/constants/vals"
	"klogga/util/testutil"
	"testing"
	"time"
)

func TestSpanString(t *testing.T) {
	span := StartLeaf(context.Background())
	span.Tag("danila", "a")
	span.Val("dan_val", 444)

	require.Empty(t, span.Component())

	str := span.Stringify()

	require.Contains(t, str, "danila")
	require.Contains(t, str, "dan_val:'444'")
	require.Contains(t, str, "klogga.")
	require.Contains(t, str, "TestSpanString()")

	t.Log("str:", str)
}

func TestSpanDuration(t *testing.T) {
	span := StartLeaf(context.Background())
	time.Sleep(10 * time.Millisecond)
	span.Stop()
	require.True(t, span.IsFinished())
	t.Log("duration", span.Duration())
	require.InDelta(t, 10, span.Duration().Milliseconds(), 20)
}

func TestSpanIntAsString(t *testing.T) {
	span := StartLeaf(context.Background())
	span.Tag("int_value", 111)

	str := span.Stringify()
	require.Contains(t, str, "111")
	t.Logf("str: %s", str)
}

func TestSpanJson(t *testing.T) {
	span, _ := Start(context.Background())
	span.Tag("danila", "a")
	span.Val("dan_val", 444)
	span.ErrVoid(errors.New("error lalala"))

	require.Empty(t, span.Component())

	bb, err := span.Json()
	require.NoError(t, err)
	str := string(bb)
	require.Contains(t, str, "danila")
	require.Contains(t, str, "444")
	require.Contains(t, str, "klogga.")
	require.Contains(t, str, "TestSpanJson")

	t.Logf("str: %s", str)
}

func TestWriterNoParent(t *testing.T) {
	span, _ := Start(context.Background())
	span.Val("dan_val", 444)

	require.Empty(t, span.Component())

	str := span.Stringify()
	require.NotContains(t, str, "parent_id:AAAAAAAAAAAAA")

	t.Logf("str: %s", str)
}

type La struct {
}

//go:noinline
func (c La) DoStuff(ctx context.Context) (string, string, string) {
	span, _ := Start(ctx)
	return span.packageName, span.className, span.name
}

//go:noinline
func (c *La) DoStuffPointer(ctx context.Context) (string, string, string) {
	span, _ := Start(ctx)
	return span.packageName, span.className, span.name
}

func TestSpanCodeStructureFields(t *testing.T) {
	la := La{}
	p, c, f := la.DoStuff(context.Background())
	require.Equal(t, "klogga", p)
	require.Equal(t, "La", c)
	require.Equal(t, "DoStuff", f)
	p, c, f = la.DoStuffPointer(context.Background())
	require.Equal(t, "klogga", p)
	require.Equal(t, "La", c)
	require.Equal(t, "DoStuffPointer", f)
}

func TestErrorSpanString(t *testing.T) {
	err := errors.New("fail fail fail")
	tag := "test1"
	val := "val1"
	span := StartLeaf(testutil.Timeout()).Tag("tt", tag).Val("vv", val).
		ErrSpan(err)
	spanStr := span.Stringify()
	require.Contains(t, spanStr, err.Error())

	errSpanStr := CreateErrSpanFrom(testutil.Timeout(), span).Stringify()
	require.Contains(t, errSpanStr, err.Error())
	require.Contains(t, errSpanStr, tag)
	require.Contains(t, errSpanStr, val)
}

func TestNoErrorSpan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tag := "test1"
	val := "val1"
	span := StartLeaf(testutil.Timeout()).Tag("tt", tag).Val("vv", val)

	errSpan := CreateErrSpanFrom(testutil.Timeout(), span)
	require.Nil(t, errSpan)
}

func TestSpanIdString(t *testing.T) {
	span := StartLeaf(testutil.Timeout())
	spanStr := span.Stringify()
	require.Contains(t, spanStr, "id")
	require.Contains(t, spanStr, span.ID().String())
}

func TestParentSpanIdString(t *testing.T) {
	parentID := NewSpanID()
	span := StartLeaf(testutil.Timeout(), WithParentSpanID(parentID))
	spanStr := span.Stringify()
	require.Contains(t, spanStr, "id")
	require.Contains(t, spanStr, span.ID().String())
	require.Contains(t, spanStr, "parent_id")
	require.Contains(t, spanStr, parentID.String())
}

func TestSpanWarn(t *testing.T) {
	span := StartLeaf(testutil.Timeout())
	span.Warn(errors.New("warn warn warn"))
	spanStr := span.Stringify()
	require.Contains(t, spanStr, "warn warn")
}

func TestSpanDeferErr(t *testing.T) {
	span := StartLeaf(testutil.Timeout())
	span.DeferErr(errors.New("defer err"))
	spanStr := span.Stringify()
	require.Contains(t, spanStr, "defer err")
}

func TestObjectString(t *testing.T) {
	span := StartLeaf(testutil.Timeout()).ErrSpan(errors.New("fail fail fail"))
	span.ValAsObj(
		"obj", struct {
			Name string
		}{
			Name: "danila",
		},
	)
	require.Contains(t, span.Stringify(), "danila")
	require.Contains(t, span.Stringify(), "fail")
}

func TestMapObjectNesting(t *testing.T) {
	span := StartLeaf(testutil.Timeout()).ErrSpan(errors.New("fail fail fail"))

	mapObj := map[string]interface{}{
		"obj_struct": struct {
			Name string
		}{
			Name: "danila",
		},
		"int_val": 111,
	}
	span.ValAsObj(vals.ResponseBody, mapObj)

	spanStr := span.Stringify()
	t.Log("span", spanStr)
	require.Contains(t, spanStr, vals.ResponseBody)
	require.Contains(t, spanStr, "obj_struct")
	require.Contains(t, spanStr, "danila")
	require.Contains(t, spanStr, "int_val")
	require.Contains(t, spanStr, "111")
}

func TestObjectNestingInErrString(t *testing.T) {
	span := StartLeaf(testutil.Timeout()).ErrSpan(errors.New("fail fail fail"))
	span.ValAsObj(
		"obj_struct", struct {
			Name string
		}{
			Name: "danila",
		},
	)

	errSpan := CreateErrSpanFrom(testutil.Timeout(), span)
	require.NotNil(t, errSpan)
	errSpanStr := errSpan.Stringify()
	t.Log("errSpanStr", errSpanStr)
	require.Contains(t, errSpanStr, "danila")
	require.Contains(t, errSpanStr, "fail")
}

func TestSetGlobalTag(t *testing.T) {
	parentSpan, ctx := Start(context.Background())
	parentSpan.Tag("some_local_key", "local_parent_value")
	parentSpan.GlobalTag("some_key", "global_parent_value")
	span := StartLeaf(ctx).Tag("local_key", "local_child_value")
	spanStr := span.Stringify()

	require.Contains(t, spanStr, "global_parent_value")
	require.NotContains(t, spanStr, "local_parent_value")
	require.Contains(t, spanStr, "local_child_value")
}

func TestIntValue(t *testing.T) {
	span := StartLeaf(context.Background())
	span.Val(vals.Count, 444)
	str := span.Stringify()
	require.Contains(t, str, "count:'444'")
}

func TestInitHostname(t *testing.T) {
	InitHostname()
	span := StartLeaf(context.Background())

	require.NotEmpty(t, span.Host())
}

func TestSetHostname(t *testing.T) {
	SetHostname("test_host")
	span := StartLeaf(context.Background())

	require.Equal(t, "test_host", span.Host())
}

func TestOptions(t *testing.T) {
	dt, _ := time.Parse(TimestampLayout, "2006-01-02 15:04:05.000")
	tt := NewTraceID()
	span := StartLeaf(
		context.Background(),
		WithTraceID(tt),
		WithName("danilas"),
		WithTimestamp(dt),
	)
	res := span.Stringify()
	t.Log("span:", res)
	require.Contains(t, res, "2006-01-02")
	require.Contains(t, res, "danilas")
	require.Contains(t, res, tt.String())
}

func TestMessageBasic(t *testing.T) {
	span := Message("lalala")
	res := span.Stringify()
	require.Contains(t, res, "lalala")
	require.Contains(t, res, " I ")
}

type impl struct {
}

func (i impl) DoSomething() {
}

func TestPgValueJsonbNil(t *testing.T) {
	span := StartLeaf(testutil.Timeout())

	type tester interface {
		DoSomething()
	}
	zzNil := func() tester {
		var rr *impl
		return rr
	}()

	span.ValAsObj("is_nil", zzNil)

	_, ok1 := span.Vals()["is_nil"]
	require.False(t, ok1)
}
