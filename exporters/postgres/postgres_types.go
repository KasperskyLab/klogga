package postgres

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"klogga"
	"reflect"
	"strings"
	"time"
)

const PgTextTypeName = "text"
const PgJsonbTypeName = "jsonb"

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
		return PgTextTypeName, v
	case time.Time:
		return "timestamp without time zone", v.UTC()
	case time.Duration:
		return "bigint", v
	case *klogga.ObjectVal:
		data, err := json.Marshal(v)
		if err != nil {
			return PgTextTypeName, fmt.Sprintf("%v", v)
		}
		return PgJsonbTypeName, string(data)
	case error:
		return PgTextTypeName, v.Error()
	case fmt.Stringer:
		return PgTextTypeName, v.String()
	default:
		if v != reflect.Struct {
			data, err := json.Marshal(v)
			if err != nil {
				return PgTextTypeName, fmt.Sprintf("%v", v)
			}
			return PgJsonbTypeName, data
		}
		return PgTextTypeName, fmt.Sprintf("%v", v)
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
	Table          string
	Span           *klogga.Span
	Column         ColumnSchema
	ExistingColumn ColumnSchema
}

func newErrDescriptor(table string, span *klogga.Span, col, existingCol ColumnSchema) ErrDescriptor {
	return ErrDescriptor{
		Table:          table,
		Span:           span,
		Column:         col,
		ExistingColumn: existingCol,
	}
}

func (e *ErrDescriptor) Err() error {
	return errors.Errorf(
		"bad span column (%s): '%v' column type is %v; %v type is expected",
		e.Table, e.Column.Name, e.Column.DataType, e.ExistingColumn.DataType,
	)
}

func (e *ErrDescriptor) Warn() error {
	return e.Span.Warns()
}

func toPgColumnName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "-", "_"))
}
