package testutil

import (
	"context"
	"time"
)

func Timeout() context.Context {
	//nolint
	timeout, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return timeout
}
