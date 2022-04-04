package pg_exporter

import (
	"fmt"
	"strings"
)

// TableSchema key is the table name
type TableSchema struct {
	columns      []*ColumnSchema
	columnsNames []string
	mm           map[string]*ColumnSchema
}

func NewTableSchema(columns []*ColumnSchema) *TableSchema {
	mm := make(map[string]*ColumnSchema)
	columnsNames := make([]string, 0, len(columns))
	for _, col := range columns {
		mm[col.Name] = col
		columnsNames = append(columnsNames, col.Name)
	}
	return &TableSchema{
		columns:      columns,
		columnsNames: columnsNames,
		mm:           mm,
	}
}

func (t TableSchema) ColumnsCount() int {
	return len(t.columns)
}

func (t TableSchema) ColumnNames() []string {
	return t.columnsNames
}

// GetAlterSchema returns missing columns' schema
// returns spans that cannot be written just by adding columns
func (t TableSchema) GetAlterSchema(dataset RecordSet) (*TableSchema, []ErrDescriptor) {
	alterSchema := NewTableSchema([]*ColumnSchema{})
	errSpans := make([]ErrDescriptor, 0)

	for _, colSchema := range dataset.Schema.Columns() {
		existingColSchema, found := t.Column(colSchema.Name)
		if !found {
			alterSchema.AddColumn(
				ColumnSchema{
					Name:     toPgColumnName(colSchema.Name),
					DataType: colSchema.DataType,
				},
			)
			continue
		}
		if existingColSchema.DataType != colSchema.DataType {
			for _, span := range dataset.Spans {
				val := findColumnValue(span, colSchema.Name)
				pgType, _ := GetPgTypeVal(val)
				if !strings.EqualFold(pgType, existingColSchema.DataType) {
					errSpans = append(errSpans, NewErrDescriptor(span, *colSchema, *existingColSchema))
				}
			}
		}
	}
	return alterSchema, errSpans
}

func (t *TableSchema) Merge(newCols []*ColumnSchema) *TableSchema {
	tCopy := NewTableSchema(t.columns)
	for _, colSchema := range newCols {
		tCopy.AddColumn(*colSchema)
	}
	return tCopy
}

func (t TableSchema) Column(name string) (*ColumnSchema, bool) {
	col, ok := t.mm[name]
	return col, ok
}

func (t *TableSchema) AddColumn(col ColumnSchema) {
	if _, ok := t.Column(col.Name); !ok {
		t.columns = append(t.columns, &col)
		t.columnsNames = append(t.columnsNames, col.Name)
	}
	t.mm[col.Name] = &col
}

func (t TableSchema) Columns() []*ColumnSchema {
	return t.columns
}

func (t TableSchema) InsertStatement(schema string, table string) string {
	paramsStr := "$1"
	for i := 2; i <= t.ColumnsCount(); i++ {
		paramsStr += fmt.Sprintf(",$%v", i)
	}
	return fmt.Sprintf(
		"INSERT INTO %v.%v (%s) VALUES (%s)",
		schema, table,
		strings.Join(t.ColumnNames(), ","),
		paramsStr,
	)
}

func (t TableSchema) CreateTableStatement(schema, tableName string, useTimescale bool) string {
	b := strings.Builder{}

	b.WriteString(fmt.Sprintf("CREATE TABLE %s.%s\n", schema, tableName))
	b.WriteString("(\n")

	for i, column := range t.columns {
		if i != 0 {
			b.WriteString(",\n")
		}
		b.WriteString(fmt.Sprintf("%v %v", column.Name, column.DataType))
	}
	b.WriteString(");\n")

	if useTimescale {
		b.WriteString(fmt.Sprintf("SELECT create_hypertable('%v.%v','time');\n", schema, tableName))
	}

	return b.String()
}

func (t TableSchema) AlterTableStatement(schema, tableName string) string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("ALTER TABLE %v.%v\n", schema, tableName))

	for i, column := range t.columns {
		b.WriteString(fmt.Sprintf("  ADD COLUMN %v %v", column.Name, column.DataType))
		if i == len(t.columns)-1 {
			b.WriteString(";\n")
		} else {
			b.WriteString(",\n")
		}
	}
	return b.String()
}

type ColumnSchema struct {
	Name        string
	DataType    string
	TypePostfix string
	IsNullable  bool
}

func (s ColumnSchema) Sql() string {
	nullableStr := "null"
	if !s.IsNullable {
		nullableStr = "not " + nullableStr
	}
	return fmt.Sprintf("%s\t%s %s %s", s.Name, s.DataType, s.TypePostfix, nullableStr)
}
