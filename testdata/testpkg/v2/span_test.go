package testpkg

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVersionedPackageName(t *testing.T) {
	span := CreateMySpan()
	require.Equal(t, "testpkg", span.Package())
}
