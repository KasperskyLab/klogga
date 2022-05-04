package otel

import (
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"klogga"
)

func ConvertValue(val interface{}) attribute.Value {
	switch typed := val.(type) {
	case bool:
		return attribute.BoolValue(typed)
	case int:
		return attribute.IntValue(typed)
	case int64:
		return attribute.Int64Value(typed)
	case float32:
		return attribute.Float64Value(float64(typed))
	case float64:
		return attribute.Float64Value(typed)
	case string:
		return attribute.StringValue(typed)
	case []bool:
		return attribute.BoolSliceValue(typed)
	case []int:
		return attribute.IntSliceValue(typed)
	case []int64:
		return attribute.Int64SliceValue(typed)
	case []float64:
		return attribute.Float64SliceValue(typed)
	case []string:
		return attribute.StringSliceValue(typed)
	case fmt.Stringer:
		return attribute.StringValue(typed.String())
	default:
		return attribute.StringValue(klogga.ValObject(typed).String())
	}
}
