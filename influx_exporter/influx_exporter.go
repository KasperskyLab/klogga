//go:generate mockgen -source=influx_exporter.go -destination=influx_exporter_mocks.go -package=influx_exporter

package influx_exporter

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb1-client"
	"github.com/pkg/errors"
	"go.kl/klogga"
	"go.kl/klogga/constants/vals"
	"math"
	"strings"
)

type Tracer struct {
	client InfluxClient
	errTrs klogga.Tracer
	conf   *Conf
}

type Conf struct {
	// defaultMeasurement is used when spans don't have its component set Prefix is
	// added to the measurement name
	DefaultMeasurement string
	Prefix             string
	Database           string
	Precision          string
}

func (c *Conf) PrecisionOrDefault() string {
	if c.Precision == "" {
		return "ns"
	}
	return c.Precision
}

// InfluxClient separates tracer from influx implementation, in case something would be needed in between
// (although it still depends on specific influx client types)
type InfluxClient interface {
	Write(bp client.BatchPoints) (*client.Response, error)
}

// New not an actual tracer, should be used with Batcher
// conf - configures tracer, use empty struct by default
// client - influx client implementation, no v2 support (yet)
// errTrs - tracer, where influx errors will be written. Be careful about possible recursion!
func New(conf *Conf, client InfluxClient, errTrs klogga.Tracer) *Tracer {
	return &Tracer{
		client: client,
		conf:   conf,
		errTrs: errTrs,
	}
}

func (t *Tracer) Write(ctx context.Context, spans []*klogga.Span) error {
	points := make([]client.Point, 0, len(spans))

	for _, span := range spans {
		span.Stop()

		tags := make(map[string]string)
		for key, val := range span.Tags() {
			tags[key] = fmt.Sprintf("%v", val)
		}
		fillBaseTags(tags, span)

		fields := make(map[string]interface{})
		for key, val := range span.Vals() {
			fields[key] = AdjustValType(val)
		}
		fields[vals.SpanId] = span.ID().String()

		errFlag := ""
		if span.Errs() != nil {
			fields[("error")] = span.Errs().Error()
			errFlag = "e"
		}
		if span.Warns() != nil {
			fields[("warn_error")] = span.Warns().Error()
			errFlag += "w"
		}
		if span.DeferErrs() != nil {
			fields[("warn_error")] = span.DeferErrs().Error()
			errFlag += "w"
		}
		if errFlag != "" {
			tags[("err")] = errFlag
		}

		// duration
		dur := span.Duration()
		fields["duration"] = dur.Seconds() * 1000
		// also, gotta add a rounded value to tags for fast filtration
		tags["dur"] = klogga.RoundDur(dur)

		points = append(
			points, client.Point{
				Measurement: t.measurement(span),
				Tags:        tags,
				Time:        span.StartedTs(),
				Fields:      fields,
				Precision:   t.conf.PrecisionOrDefault(),
			},
		)
	}

	bp := client.BatchPoints{
		Points:    points,
		Database:  t.conf.Database,
		Precision: t.conf.PrecisionOrDefault(),
	}

	if _, err := t.client.Write(bp); err != nil {
		// Errors should include source span data, or even line protocol data
		// but looks like it should be done inside influx client
		klogga.StartLeaf(context.Background()).
			ErrSpan(errors.Wrapf(err, "Unable to write influx batch: %+v", bp)).
			FlushTo(t.errTrs)
	}
	return nil
}

func (t *Tracer) measurement(span *klogga.Span) string {
	measurement := strings.ToLower(span.Component().String())
	if measurement == "" {
		measurement = span.Package()
	}
	if measurement == "" {
		measurement = t.conf.DefaultMeasurement
	}
	if t.conf.Prefix != "" && !strings.HasPrefix(measurement, t.conf.Prefix) {
		return t.conf.Prefix + "_" + measurement
	}
	return measurement
}

func (t *Tracer) Shutdown(context.Context) error {
	return nil
}

// AdjustValType changes the go type to compatible go type, supported by influx
func AdjustValType(val interface{}) interface{} {
	switch tval := val.(type) {
	case uint64:
		if tval <= math.MaxInt64 {
			return int64(tval)
		}
		return -math.MaxInt64
	case string, bool, nil:
		return tval
	case int, int8, int16, int32, int64:
		return tval
	case uint, uint8, uint16, uint32:
		return tval
	case float32, float64:
		return tval
	case error:
		return tval.Error()
	case fmt.Stringer:
		return tval.String()
	default:
		return fmt.Sprintf("%v", tval)
	}
}

func fillBaseTags(tags map[string]string, span *klogga.Span) {
	tags["host"] = span.Host()
	tags["class"] = span.PackageClass()
	tags["func"] = span.Name()
}
