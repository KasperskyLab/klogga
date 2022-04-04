package grpc_adapter

import (
	"context"
	"fmt"
	"go.kl/klogga"
)

type GrpcLoggerV2Adapter struct {
	trs  klogga.Tracer
	conf *GrpcLoggerConf
}

type GrpcLoggerConf struct {
	// 0 - log everything
	// 4 - log nothing
	VerbosityLevel klogga.LogLevel
}

func NewGrpcLoggerV2Adapter(trs klogga.Factory, conf *GrpcLoggerConf) *GrpcLoggerV2Adapter {
	return &GrpcLoggerV2Adapter{trs: trs.Named("grpc"), conf: conf}
}

func (g GrpcLoggerV2Adapter) trace(level klogga.LogLevel, mes string) {
	span := klogga.StartLeaf(context.Background())
	defer g.trs.Finish(span)
	span.Level(level).Val("message", mes)
}

func (g GrpcLoggerV2Adapter) Info(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Infoln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Infof(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Info {
		return
	}
	g.trace(klogga.Info, fmt.Sprintf(format, args...))
}

func (g GrpcLoggerV2Adapter) Warning(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Warningln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Warningf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Warn {
		return
	}
	g.trace(klogga.Warn, fmt.Sprintf(format, args...))
}

func (g GrpcLoggerV2Adapter) Error(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Errorln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Errorf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Error {
		return
	}
	g.trace(klogga.Error, fmt.Sprintf(format, args...))
}

func (g GrpcLoggerV2Adapter) Fatal(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Fatalln(args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprint(args...))
}

func (g GrpcLoggerV2Adapter) Fatalf(format string, args ...interface{}) {
	if g.conf.VerbosityLevel <= klogga.Fatal {
		return
	}
	g.trace(klogga.Fatal, fmt.Sprintf(format, args...))
}

func (g GrpcLoggerV2Adapter) V(l int) bool {
	if g.conf.VerbosityLevel == 0 {
		return true
	}
	return l >= int(g.conf.VerbosityLevel)
}
