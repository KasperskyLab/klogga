# Introduction 
Opinionated logging-audit-tracing library.
Data collected via klogga can be configured to be exported to different sources, including traditional text logs, but with emphasis on structured storages, primarily time-series databases and Open Telemetry.

# Getting Started
import klogga
configure your root tracer, don't forget to use batcher for buffered traces
see examples/tracers_tree.go
