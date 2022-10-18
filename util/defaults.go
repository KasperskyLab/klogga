package util

import (
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/exporters/golog"
)

func DefaultFactory() *klogga.Factory {
	return klogga.NewFactory(golog.New(nil))
}
