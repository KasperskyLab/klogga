package stringutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCutCenter(t *testing.T) {

	cutCenter := MaxLenCutCenter("0123456789", 8)

	if cutCenter != "01....89" {
		t.Fatal("cutCenter=" + cutCenter)
	}
}

func TestToSnakeCase(t *testing.T) {
	assert.Equal(t, "test_string", ToSnakeCase("TestString"))
	assert.Equal(t, "test_string", ToSnakeCase("testString"))
	assert.Equal(t, "test_string", ToSnakeCase("test string"))
}
