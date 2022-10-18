package deep

import (
	"context"
	"github.com/KasperskyLab/klogga"
)

type Logger struct{}

func (r *Logger) Log(trsFactory klogga.TracerProvider) {
	klogga.StartLeaf(context.Background()).Val("run", "test").FlushTo(trsFactory.NamedPkg())
}

func (r *Logger) Log2(trsFactory klogga.TracerProvider) {
	span, _ := klogga.Start(context.Background())
	span.Val("run", "test").FlushTo(trsFactory.NamedPkg())
}

func (r *Logger) Message(trsFactory klogga.TracerProvider) {
	klogga.Message("message").FlushTo(trsFactory.NamedPkg())
}
