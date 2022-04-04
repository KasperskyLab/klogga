package pg_exporter

import (
	"context"
	"database/sql"
	"github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.kl/klogga"
	"go.kl/klogga/constants/vals"
	"go.kl/klogga/golog_exporter"
	"go.kl/klogga/util/errs"
	"go.kl/klogga/util/reflectutil"
	"strings"
	"sync"
	"time"
)

//go:generate mockgen -source=pg_exporter.go -destination=pg_exporter_mock.go -package=pg_exporter

const (
	defaultSchema         = "audit"
	errorMetricsTableName = "error_metrics"
	defaultWriteTimeout   = time.Second

	msgUnableToConnectPG        = "unable to connect PG"
	msgUnableToBeginTx          = "unable to begin TX"
	msgUnableToCommitTx         = "unable to commit TX"
	msgUnableToPrepareStatement = "unable to prepare statement"
	msgUnableToExecStatement    = "unable to exec statement"
	msgUnableToQuery            = "unable to query"
	magUnableToScan             = "unable to scan"
	msgUnableToExec             = "unable to exec"
)

type Conf struct {
	SchemaName   string
	WriteTimeout time.Duration
	UseTimescale bool
}

type Connection interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Connector provides PG connections in an abstract way
type Connector interface {
	GetConnection(ctx context.Context) (Connection, error)
	// Close for cleanup, will be  called when tracer closes
	Close(ctx context.Context) error
}

// Exporter writes Spans to postgres
// can create tables and add columns to table
type Exporter struct {
	cfg         *Conf
	connFactory Connector
	psql        squirrel.StatementBuilderType
	errorTracer klogga.Tracer

	sysCols  *TableSchema
	errTable *TableSchema

	tables     map[string]*TableSchema
	loadSchema sync.Once
}

// New to be used with batcher
// cfg - config
// connFactory - connection to PG
// errorTracer - an alternative tracer to write its own errors to
func New(cfg *Conf, connFactory Connector, errorTracer klogga.Tracer) *Exporter {
	if errorTracer == nil {
		errorTracer = klogga.NewFactory(golog.New(nil)).NamedPkg()
		klogga.StartLeaf(context.Background()).Warn(errors.New("using default golog tracer for pg errors")).
			FlushTo(errorTracer)
	}

	if cfg.SchemaName == "" {
		cfg.SchemaName = defaultSchema
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}

	sysCols := NewTableSchema(
		[]*ColumnSchema{
			{"time", "timestamp without time zone", "default timezone('UTC'::text, statement_timestamp())", false},
			{"id", "uuid", "", false},
			{"host", "text", "", false},
			{"pkg_class", "text", "", false},
			{"name", "text", "", false},
			{"parent", "uuid", "", true},
			{"error", "text", "", true},
			{"warn", "text", "", true},
			{"duration", "bigint", "", false},
		},
	)
	errTable := NewTableSchema(
		append(
			sysCols.Columns(), &ColumnSchema{
				"component", "text", "", false,
			},
		),
	)

	return &Exporter{
		cfg:         cfg,
		connFactory: connFactory,
		psql:        squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
		errorTracer: errorTracer,
		sysCols:     sysCols,
		errTable:    errTable,
		tables:      make(map[string]*TableSchema),
		loadSchema:  sync.Once{},
	}
}

func (e *Exporter) Stop(ctx context.Context) error {
	return e.Shutdown(ctx)
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	pgErr := e.connFactory.Close(ctx)
	return pgErr
}

func (e *Exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.WriteTimeout)
	defer cancel()
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)

	if len(spans) == 0 {
		return nil
	}

	// On first write cache schema
	// TODO maybe reload the schema periodically
	e.loadSchema.Do(
		func() {
			if err := e.loadSchemas(ctx); err != nil {
				e.loadSchema = sync.Once{}
			}
		},
	)

	datasets, errSpans := e.CreateRecordSets(spans...)
	e.writeErrSpans(ctx, errSpans)
	for tableName, dataset := range datasets {
		schema, found := e.tables[tableName]
		if !found {
			if err := e.createTable(ctx, tableName, e.sysCols.Merge(dataset.Schema.Columns())); err != nil {
				span.ErrVoid(err)
				continue
			}
			e.tables[tableName] = dataset.Schema
		} else {
			alterSchema, errSpans := schema.GetAlterSchema(dataset)
			if len(errSpans) > 0 {
				e.writeErrSpans(ctx, errSpans)
				continue
			}
			if err := e.alterTable(ctx, tableName, alterSchema); err != nil {
				span.ErrVoid(err)
				continue
			}
			e.tables[tableName] = e.tables[tableName].Merge(alterSchema.Columns())
		}
		e.writeDataset(ctx, tableName, dataset)
	}

	return nil
}

func (e *Exporter) writeDataset(ctx context.Context, tableName string, dataset RecordSet) {
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)
	span.Tag("table", tableName)

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToConnectPG))
		return
	}

	isCommitted := false
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToBeginTx))
		return
	}
	defer func() {
		if !isCommitted {
			span.DeferErr(tx.Rollback())
		}
	}()

	query := pq.CopyInSchema(e.cfg.SchemaName, tableName, dataset.Schema.ColumnNames()...)
	span.Val(vals.Query, query)
	span.Val("columns_count", dataset.Schema.ColumnsCount())
	stmt, err := tx.Prepare(query)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToPrepareStatement))
		return
	}
	defer func() {
		span.DeferErr(errors.Wrap(stmt.Close(), "unable to close statement"))
	}()

	for _, span := range dataset.Spans {
		strErr := ""
		if sErr := errs.Append(span.Errs(), span.DeferErrs()); sErr != nil {
			strErr = sErr.Error()
		}
		strWarn := ""
		if sErr := span.Warns(); sErr != nil {
			strWarn = sErr.Error()
		}

		vv := []interface{}{
			span.StartedTs(),
			span.ID().AsUUID(),
			span.Host(),
			span.PackageClass(),
			span.Name(),
			span.ParentID().AsNullableUUID(),
			strErr,
			strWarn,
			span.Duration(),
		}

		for _, colName := range dataset.Schema.ColumnNames()[e.sysCols.ColumnsCount():] {
			val := findColumnValue(span, colName)
			if reflectutil.IsNil(val) {
				vv = append(vv, nil)
				continue
			}
			_, pgVal := GetPgTypeVal(val)
			vv = append(vv, pgVal)
		}

		if _, err := stmt.ExecContext(ctx, vv...); err != nil {
			span.ErrVoid(errors.Wrap(err, msgUnableToExecStatement))
			continue
		}
	}

	// Due to asynchronous nature of "Exec" need to call Exec(nil) to sync the COPY stream
	// and to get any errors from pending data
	if _, err = stmt.Exec(); err != nil {
		span.ErrVoid(errors.Wrap(err, "unable to finish COPY statement"))
		return
	}

	if err := tx.Commit(); err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToCommitTx))
	}
	isCommitted = true
}

func (e *Exporter) createTable(ctx context.Context, tableName string, schema *TableSchema) error {
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)

	q := schema.CreateTableStatement(e.cfg.SchemaName, tableName, e.cfg.UseTimescale)
	span.
		Tag("table", tableName).
		Val("columns_count", schema.ColumnsCount()).
		Val("columns", strings.Join(schema.ColumnNames(), ","))

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToConnectPG))
	}

	if _, err := conn.ExecContext(ctx, q); err != nil {
		span.Val(vals.Query, q)
		return span.Err(errors.Wrap(err, msgUnableToExec))
	}
	return nil
}

func (e *Exporter) alterTable(ctx context.Context, tableName string, schema *TableSchema) error {
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)
	if schema.ColumnsCount() <= 0 {
		return nil
	}

	q := schema.AlterTableStatement(e.cfg.SchemaName, tableName)
	span.
		Tag("table", tableName).
		Val("columns_count", schema.ColumnsCount()).
		Val(vals.Query, q)

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToConnectPG))
	}
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return span.Err(errors.Wrap(err, msgUnableToExec))
	}
	return nil
}

func (e *Exporter) loadSchemas(ctx context.Context) error {
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToConnectPG))
	}

	query, args := e.psql.Select("table_name", "column_name", "is_nullable", "data_type").
		From("information_schema.columns").
		Where(squirrel.Eq{"table_schema": e.cfg.SchemaName}).
		MustSql()

	span.Val(vals.Query, query)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToQuery))
	}
	if err := rows.Err(); err != nil {
		return span.Err(errors.Wrap(err, msgUnableToQuery))
	}
	defer func() {
		span.DeferErr(rows.Close())
	}()

	for rows.Next() {
		var tableName string
		var columnName string
		var isNullable string
		var dataType string

		if err := rows.Scan(&tableName, &columnName, &isNullable, &dataType); err != nil {
			return span.Err(errors.Wrap(err, magUnableToScan))
		}

		if _, found := e.tables[tableName]; !found {
			e.tables[tableName] = NewTableSchema([]*ColumnSchema{})
		}

		colSchema := ColumnSchema{
			Name:     columnName,
			DataType: dataType,
		}
		e.tables[tableName].AddColumn(colSchema)
	}
	span.Val("table_count", len(e.tables))
	return nil
}

func (e *Exporter) writeErrSpans(ctx context.Context, errDescrs []ErrDescriptor) {
	if len(errDescrs) == 0 {
		return
	}
	span, ctx := klogga.Start(ctx)
	defer e.finishLogSpan(span)
	span.Val(vals.Count, len(errDescrs))

	_, ok := e.tables[errorMetricsTableName]
	if !ok {
		err := e.createTable(ctx, errorMetricsTableName, e.errTable)
		if err != nil {
			span.ErrVoid(errors.Wrap(err, "failed to create table for errors"))
			return
		}
	}

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToConnectPG))
		return
	}

	statementText := e.errTable.InsertStatement(e.cfg.SchemaName, errorMetricsTableName)

	for _, errDescr := range errDescrs {
		warn := ""
		if errDescr.Warn() != nil {
			warn = errDescr.Warn().Error()
		}
		_, err := conn.ExecContext(
			ctx,
			statementText,
			errDescr.Span.StartedTs(),
			errDescr.Span.ID().AsUUID(),
			errDescr.Span.Host(),
			errDescr.Span.PackageClass(),
			errDescr.Span.Name(),
			errDescr.Span.ParentID().AsNullableUUID(),
			errDescr.Err().Error(),
			warn,
			errDescr.Span.Duration(),
			errDescr.Span.Component(),
		)
		if err != nil {
			span.Val(vals.Query, statementText).ErrVoid(errors.Wrap(err, "unable to write bad span"))
		}
	}
}

func (e *Exporter) finishLogSpan(span *klogga.Span) {
	if span.HasErr() || span.HasWarn() || span.HasDeferErr() {
		e.errorTracer.Finish(span)
	}
}

type RecordSet struct {
	Schema *TableSchema
	Spans  []*klogga.Span
}

// CreateRecordSets creates record sets from spans, grouped by table name
// if span column can't be placed in the structure, it is added to []ErrDescriptor
// (this happens when span has a data type in tag/val that is different from the expected type
// in the existing structure)
func (e *Exporter) CreateRecordSets(spans ...*klogga.Span) (map[string]RecordSet, []ErrDescriptor) {
	datasets := make(map[string]RecordSet)
	errSpans := make([]ErrDescriptor, 0)

	for _, span := range spans {
		tableName := spanTableName(span)
		dataset, found := datasets[tableName]
		if !found {
			dataset = RecordSet{
				Schema: NewTableSchema(e.sysCols.Columns()),
				Spans:  make([]*klogga.Span, 0),
			}
		}

		for name, val := range e.getSpanVals(span) {
			name = toPgColumnName(name)
			col, found := dataset.Schema.Column(name)
			valType, _ := GetPgTypeVal(val)
			newColumn := ColumnSchema{
				Name:     name,
				DataType: valType,
			}
			if !found {
				dataset.Schema.AddColumn(newColumn)
				continue
			}
			if !strings.EqualFold(newColumn.DataType, col.DataType) {
				errSpans = append(errSpans, NewErrDescriptor(span, newColumn, *col))
			}
		}

		if len(errSpans) <= 0 {
			dataset.Spans = append(dataset.Spans, span)
		}

		datasets[tableName] = dataset
	}

	return datasets, errSpans
}

func spanTableName(span *klogga.Span) string {
	table := span.Component().String()
	if table == "" {
		table = span.Package()
	}
	return table
}

// merges span tabs and values into a single map
// excludes system columns
// converts column name to a standard PG name
func (e *Exporter) getSpanVals(span *klogga.Span) map[string]interface{} {
	result := make(map[string]interface{})
	for name, val := range span.Tags() {
		if _, isSysCol := e.sysCols.Column(name); isSysCol {
			continue
		}
		result[name] = val
	}
	for name, val := range span.Vals() {
		if _, isSysCol := e.sysCols.Column(name); isSysCol {
			continue
		}
		result[name] = val
	}
	return result
}
