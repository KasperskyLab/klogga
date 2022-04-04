package errs

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAppend(t *testing.T) {
	require.Nil(t, Append(nil, nil))
	require.NotNil(t, Append(errors.New("err"), nil))
	require.NotNil(t, Append(nil, errors.New("err")))
}

func TestAppenManyNils(t *testing.T) {
	source := errors.New("err")
	err := Append(nil, nil, nil, source)
	require.NotNil(t, err)
	require.Error(t, source, err)
}
