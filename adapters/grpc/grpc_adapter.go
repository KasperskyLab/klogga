package grpc

import (
	"context"
	"fmt"
	"github.com/KasperskyLab/klogga"
)

type LoggerV2 struct {
	trs  klogga.Tracer
	conf *Conf
}

type Conf struct {
	// 0 - log everything
	// 4 - log nothing
	VerbosityLevel klogga.LogLevel
}

func NewLoggerV2(trs klogga.TracerProvider, conf *Conf) *LoggerV2 {
	return &LoggerV2{trs: trs.Named("grpc"), conf: conf}
}

func (g LoggerV2) trace(level klogga.LogLevel, mes string) {
	span := klogga.StartLeaf(context.Background())
	defer g.trs.Finish(span)
	span.Level(level).Val("message", mes)
}

func (g LoggerV2) Info(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprint(args...))
}

func (g LoggerV2) Infoln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprint(args...))
}

func (g LoggerV2) Infof(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprintf(format, args...))
}

func (g LoggerV2) Warning(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprint(args...))
}

func (g LoggerV2) Warningln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprint(args...))
}

func (g LoggerV2) Warningf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprintf(format, args...))
}

func (g LoggerV2) Error(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprint(args...))
}

func (g LoggerV2) Errorln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprint(args...))
}

func (g LoggerV2) Errorf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprintf(format, args...))
}

func (g LoggerV2) Fatal(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprint(args...))
}

func (g LoggerV2) Fatalln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprint(args...))
}

func (g LoggerV2) Fatalf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprintf(format, args...))
}

func (g LoggerV2) V(l int) bool {
	if g.conf.VerbosityLevel == 0 {
		return true
	}
	return l >= int(g.conf.VerbosityLevel)
}
