# Postgres tracer
Implementation of klogga Tracer that writes spans to Postgres database with TimescaleDB extension.
Schema is created automatically, at least most of the time.
Re

## Implementation features
 - batches spans
 - creates missing tables (be careful with tracer names)
 - modifies table columns, although not all cases are supported
 - reports errors when data type for the already created column does not match that data in the span 
