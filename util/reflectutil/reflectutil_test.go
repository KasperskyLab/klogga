package reflectutil

import (
	"github.com/stretchr/testify/require"
	"testing"
)

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
		p, c, f := GetPackageClassFunc()
		require.Equal(t, "reflectutil", p)
		require.Equal(t, "", c)
		require.Equal(t, "TestGetFunc", f)
	}()
}

// go:noinline
func getClass() string {
	_, c, _ := GetPackageClassFunc()
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
