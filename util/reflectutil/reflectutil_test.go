package reflectutil

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsNil(t *testing.T) {
	var (
		nilObj       *int
		nilInterface interface{}
		nilSlice     []int
		nilMap       map[int]int
		nilChan      chan int
		nilFunc      func()
	)
	for _, obj := range []interface{}{nilObj, nilInterface, nilSlice, nilMap, nilChan, nilFunc} {
		require.True(t, IsNil(obj), obj)
	}

	var (
		number           int
		str              string
		floatNum         float32
		b                byte
		pointer          = &nilObj
		array            [2]int
		noneNilMap                   = make(map[int]int)
		noneNilSlice                 = make([]int, 0)
		noneNilChan                  = make(chan int)
		noneNilInterface interface{} = b
		noneNilFunc                  = func() {}
	)
	for _, obj := range []interface{}{
		number, str, floatNum, b, pointer, array, noneNilMap, noneNilSlice, noneNilChan, noneNilInterface, noneNilFunc,
	} {
		require.False(t, IsNil(obj), obj)
	}
}

func TestGetClass(t *testing.T) {
	c := La{}
	require.Equal(t, "La", c.DoStuff())
	require.Equal(t, "La", c.DoStuffPointer())

	cp := &c
	require.Equal(t, "La", cp.DoStuff())
	require.Equal(t, "La", cp.DoStuffPointer())
}

func TestGetFunc(t *testing.T) {
	func() {
		p, c, f := GetPackageClassFunc(2)
		require.Equal(t, "reflectutil", p)
		require.Equal(t, "", c)
		require.Equal(t, "TestGetFunc", f)
	}()
}

// go:noinline
func getClass() string {
	_, c, _ := GetPackageClassFunc(2)
	return c
}

type La struct {
	cl string
}

//go:noinline
func (c La) DoStuff() string {
	return getClass()
}

//go:noinline
func (c *La) DoStuffPointer() string {
	return getClass()
}
