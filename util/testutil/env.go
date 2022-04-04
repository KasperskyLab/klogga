package testutil

import (
	"os"
	"testing"
)

func IntegrationEnv(t *testing.T, name string) string {
	result := os.Getenv(name)
	if result == "" {
		t.Skipf("%s environment variable is required to run this test", name)
	}
	return result
}
