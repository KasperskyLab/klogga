package klogga

// SpanDefaults is applied to all spans by default, before all other options
// initially created in response to https://github.com/KasperskyLab/klogga/issues/5
// modify to change the default options
var SpanDefaults = []SpanOption{
	WithNewSpanID(),
	WithTimestampNow(),
	WithHostName(),
}
