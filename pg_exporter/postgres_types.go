package pg_exporter

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"go.kl/klogga"
	"reflect"
	"strings"
	"time"
)

const pgTextTypeName = "TEXT"
const pgJsonbTypeName = "JSONB"

// GetPgTypeVal converts go type to a compatible PG type
// structs are automatically converted to jsonb
func GetPgTypeVal(a interface{}) (string, interface{}) {
	switch v := a.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "bigint", v
	case bool:
		return "boolean", v
	case []byte:
		return "bytea", v
	case float32, float64:
		return "float8", v
	case string:
		return pgTextTypeName, v
	case time.Time:
		return "timestamp without time zone", v.UTC()
	case time.Duration:
		return "bigint", v
	case *klogga.ObjectVal:
		data, err := json.Marshal(v)
		if err != nil {
			return pgTextTypeName, fmt.Sprintf("%v", v)
		}
		return pgJsonbTypeName, string(data)
	case error:
		return pgTextTypeName, v.Error()
	case fmt.Stringer:
		return pgTextTypeName, v.String()
	default:
		if v != reflect.Struct {
			data, err := json.Marshal(v)
			if err != nil {
				return pgTextTypeName, fmt.Sprintf("%v", v)
			}
			return pgJsonbTypeName, data
		}
		return pgTextTypeName, fmt.Sprintf("%v", v)
	}
}

// finds column value by name in tags of vals of the span
func findColumnValue(span *klogga.Span, name string) interface{} {
	val, found := span.Tags()[name]
	if found {
		return val
	}
	val, found = span.Vals()[name]
	if found {
		return val
	}
	return nil
}

// ErrDescriptor describes a problematic span column, it's description will be written to error_metrics
type ErrDescriptor struct {
	Span           *klogga.Span
	Column         ColumnSchema
	ExistingColumn ColumnSchema
}

func NewErrDescriptor(span *klogga.Span, col, existingCol ColumnSchema) ErrDescriptor {
	return ErrDescriptor{
		Span:           span,
		Column:         col,
		ExistingColumn: existingCol,
	}
}

func (bs *ErrDescriptor) Err() error {
	return errors.Errorf(
		"bad span column: %v is %v; %v expected",
		bs.Column.Name, bs.Column.DataType, bs.ExistingColumn.DataType,
	)
}

func (bs *ErrDescriptor) Warn() error {
	return bs.Span.Warns()
}

func toPgColumnName(name string) string {
	return strings.ToLower(strings.Replace(name, "-", "_", -1))
}
