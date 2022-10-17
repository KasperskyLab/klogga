package testutil

import (
	"context"
	"time"
)

// Timeout context with timeout for tests
// optional parameter - timeout duration, default is 5 seconds
func Timeout(d ...time.Duration) context.Context {
	dd := 5 * time.Second
	if len(d) >= 1 {
		dd = d[0]
	}
	//nolint:govet // cancel not needed in tests
	timeout, _ := context.WithTimeout(context.Background(), dd)
	return timeout
}
