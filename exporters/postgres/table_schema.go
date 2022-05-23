package postgres

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
	errDescriptors := make([]ErrDescriptor, 0)

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
					errDescriptors = append(errDescriptors, newErrDescriptor("", span, *colSchema, *existingColSchema))
				}
			}
		}
	}
	return alterSchema, errDescriptors
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

// InsertStatement NOT injection safe
func (t TableSchema) InsertStatement(schema string, tableName string) string {
	paramsStr := "$1"
	for i := 2; i <= t.ColumnsCount(); i++ {
		paramsStr += fmt.Sprintf(",$%v", i)
	}
	return fmt.Sprintf(
		"INSERT INTO %v.%v (%s) VALUES (%s)",
		schema, tableName,
		strings.Join(t.ColumnNames(), ","),
		paramsStr,
	)
}

// CreateTableStatement NOT injection safe
func (t TableSchema) CreateTableStatement(schema, tableName string, timeCol *ColumnSchema, useTimescale bool) string {
	b := strings.Builder{}

	b.WriteString(fmt.Sprintf("CREATE TABLE %s.%s\n", schema, tableName))
	b.WriteString("(\n")

	for i, column := range t.columns {
		if i != 0 {
			b.WriteString(",\n")
		}
		b.WriteString(fmt.Sprintf("\"%v\" %v", column.Name, column.DataType))
	}
	b.WriteString(");\n")

	if useTimescale {
		b.WriteString(fmt.Sprintf("SELECT create_hypertable('%v.%v','%s');\n", schema, tableName, timeCol.Name))
	}

	return b.String()
}

// AlterTableStatement NOT injection safe
func (t TableSchema) AlterTableStatement(schema, tableName string) string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("ALTER TABLE %v.%v\n", schema, tableName))

	for i, column := range t.columns {
		b.WriteString(fmt.Sprintf("  ADD COLUMN \"%v\" %v", column.Name, column.DataType))
		if i == len(t.columns)-1 {
			b.WriteString(";\n")
		} else {
			b.WriteString(",\n")
		}
	}
	return b.String()
}

func (t *TableSchema) IsZero() bool {
	return t.ColumnsCount() <= 0
}

type ColumnSchema struct {
	Name        string
	DataType    string
	TypePostfix string
	IsNullable  bool
}

func (s ColumnSchema) SQL() string {
	nullableStr := "null"
	if !s.IsNullable {
		nullableStr = "not " + nullableStr
	}
	return fmt.Sprintf("%s\t%s %s %s", s.Name, s.DataType, s.TypePostfix, nullableStr)
}
