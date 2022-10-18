# Introduction 
Opinionated logging-audit-tracing library.
Data collected via **klogga** can be configured to be exported to different sources, including traditional text logs, but with emphasis on structured storages, primarily time-series databases and Open Telemetry.

The ideology is explained here (in russian) [Logging as fact-stream](https://www.youtube.com/watch?v=1lcKzy6j6-k)

**klogga** is in development and as of yet does not even have a 1.0 release.
API is subject to changes, although we do not expect any.
It is used extensively in Kaspersky internal projects written in go.

# Getting Started
## Basic
Import klogga, configure your root tracer, don't forget to use batcher for buffered traces
see [examples](examples/basic_app/main.go)

```go
package main

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/util"
	"time"
)

func main() {
	// kreating a factory, with the simplest exporter
	tf := util.DefaultFactory()

	// kreating a tracer with a package name
	trs := tf.NamedPkg()

	// starting a span
	// for now, we'll ignore context
	span, _ := klogga.Start(context.Background())
	// span will be written on func exit
	defer trs.Finish(span)

	// tag - potentially indexed
	span.Tag("app", "hello-world")
	// value - for metrics, or bigger values
	span.Val("meaning", 42)
	// sleep a bit, to have us some non-zero duration
	time.Sleep(154 * time.Millisecond)
}
```
OUTPUT:
```
2022-02-03 04:05:06.789 I main [main.] main() (155.17ms); app:'hello-world'; meaning:'42'; id: 65C6p2Mq2N4; trace: 4nxwjs5WR9G9A-1-dw4cqA
```

## Extended

**klogga** comes with [fx](https://github.com/uber-go/fx) integration. <br>
See [fx example](examples/fx_app/main.go)


# Features and ideas
This list will be covered and structured in the future.

- logging basic information automatically without manual setup
- logging code information for easy search (uses reflection)
- tracing support: parent-child spans and trace-id
- opentelemetry support
- TODO go fuzz tests
- TODO trace information propagation support for HTTP, GRPC 
- TODO transport support
- TODO postgres type mapping customization
- TODO span serialization to protobuf
- TODO elasticsearch exporter (if opentelemetry is not enough)
- TODO more docs for postgreSQL & timescaleDB integration
- TODO increase test coverage (~60% to ~80%)


# Running tests
## Unit test
Unit tests do not require any environment, all tests that require environment will be skipped
```shell
go test ./...
```
 

## Integration tests
Postgresql (timescaleDB) tests can be run on any empty database.
Tests use environment variable KLOGGA_PG_CONNECTION_STRING to connect to PG.
Use [docker-compose.yml](docker-compose.yml) to start default instance.
Default setup:
```shell
podman-compose up -d
KLOGGA_PG_CONNECTION_STRING='user=postgres password=postgres sslmode=disable' go test ./...
```
