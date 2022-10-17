package testpkg

import (
	"context"
	"github.com/KasperskyLab/klogga"
)

func CreateMySpan() *klogga.Span {
	span, _ := klogga.Start(context.Background())
	return span
}
