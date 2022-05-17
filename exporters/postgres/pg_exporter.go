package postgres

import (
	"context"
	"github.com/KasperskyLab/klogga"
	"github.com/KasperskyLab/klogga/constants"
	"github.com/KasperskyLab/klogga/constants/vals"
	"github.com/KasperskyLab/klogga/exporters/golog"
	"github.com/KasperskyLab/klogga/util/errs"
	"github.com/KasperskyLab/klogga/util/reflectutil"
	"github.com/KasperskyLab/klogga/util/stringutil"
	"github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"
)

//go:generate mockgen -source=pg_exporter.go -destination=pg_exporter_mock.go -package=pg_exporter

const (
	ErrorPostgresTableName = "error_postgres"
	DefaultSchema          = "audit"
	ErrorPostgresTable     = DefaultSchema + "." + ErrorPostgresTableName

	defaultWriteTimeout         = time.Second
	msgUnableToConnectPG        = "unable to connect PG"
	msgUnableToBeginTx          = "unable to begin TX"
	msgUnableToCommitTx         = "unable to commit TX"
	msgUnableToPrepareStatement = "unable to prepare statement"
	msgUnableToExecStatement    = "unable to exec statement"
	msgUnableToQuery            = "unable to query"
	magUnableToScan             = "unable to scan"
	msgUnableToExec             = "unable to exec"
)

// Conf parameters on how klogga works with DB
// not the connection string etc.
type Conf struct {
	SchemaName string
	// applied per-table, and only for writing itself, without updating the schema
	WriteTimeout time.Duration

	// don't create any DB structure automatically
	// requires all the tables to be created beforehand, with exactly required fields
	SkipSchemaCreation bool

	LoadSchemaTimeout time.Duration
	// automatically created tables are declared as timescale hypertables
	UseTimescale bool
}

// Exporter writes Spans to postgres
// can create tables and add columns to table
type Exporter struct {
	cfg         *Conf
	connFactory Connector
	psql        squirrel.StatementBuilderType
	trs         klogga.Tracer

	timeCol  *ColumnSchema
	sysCols  *TableSchema
	errTable *TableSchema

	tables         map[string]*TableSchema
	tablesLock     *sync.Mutex
	loadSchemaOnce *sync.Once
}

// New to be used with batcher
// cfg - config
// connFactory - connection to PG
// trs - a tracer to write pg_exporter logs, like DB changes, pass nil to setup default golog tracer
func New(cfg *Conf, connFactory Connector, trs klogga.Tracer) *Exporter {
	if trs == nil {
		trs = klogga.NewFactory(golog.New(nil)).NamedPkg()
		klogga.StartLeaf(context.Background()).
			ErrSpan(errors.New("using default golog tracer for pg errors")).
			FlushTo(trs)
	}

	if cfg.SchemaName == "" {
		cfg.SchemaName = DefaultSchema
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.LoadSchemaTimeout <= 0 {
		cfg.LoadSchemaTimeout = defaultWriteTimeout * 10
	}

	// base set of columns for each table
	timeCol := &ColumnSchema{"time", "timestamp without time zone", "", false}
	sysCols := NewTableSchema(
		[]*ColumnSchema{
			timeCol,
			{"id", "bytea", "", false},
			{constants.TraceID, "uuid", "", false},
			{"host", "text", "", false},
			{"pkg_class", "text", "", false},
			{"name", "text", "", false},
			{"parent", "bytea", "", true},
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
		cfg:            cfg,
		connFactory:    connFactory,
		psql:           squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
		trs:            trs,
		timeCol:        timeCol,
		sysCols:        sysCols,
		errTable:       errTable,
		tables:         make(map[string]*TableSchema),
		tablesLock:     &sync.Mutex{},
		loadSchemaOnce: &sync.Once{},
	}
}

// Start for compatibility with Start / Stop interface
func (e *Exporter) Start(context.Context) error {
	return nil
}

// Stop for compatibility with Start / Stop interface
func (e *Exporter) Stop(ctx context.Context) error {
	return e.Shutdown(ctx)
}

func (e *Exporter) ConnFactory() Connector {
	return e.connFactory
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	span, ctx := klogga.Start(ctx)
	defer e.trs.Finish(span)
	pgErr := e.connFactory.Close(ctx)
	return span.Err(pgErr)
}

func (e *Exporter) Write(ctx context.Context, spans []*klogga.Span) error {
	span, ctx := klogga.Start(ctx)
	defer e.writeIfErr(span)

	if len(spans) == 0 {
		return nil
	}

	// On first write cache schema
	e.loadSchemaOnce.Do(
		func() {
			err := e.loadSchemas(ctx)
			if err != nil {
				e.loadSchemaOnce = &sync.Once{}
			}
		},
	)

	recordSets, errSpans := e.createRecordSets(spans...)
	e.writeErrSpans(ctx, errSpans)
	for tableName, recordSet := range recordSets {
		if !e.cfg.SkipSchemaCreation {
			err := e.updateSchema(ctx, tableName, recordSet)
			if span.Err(err) != nil {
				continue
			}
		}
		e.writeRecordSet(ctx, tableName, recordSet)
	}
	if span.HasErr() {
		// on any errors gotta trigger schema reload
		e.loadSchemaOnce = &sync.Once{}
	}

	return nil
}

func (e *Exporter) updateSchema(ctx context.Context, tableName string, dataset RecordSet) error {
	span, ctx := klogga.Start(ctx)
	defer e.writeIfErr(span)

	span.Tag("table", tableName)
	e.tablesLock.Lock()
	defer e.tablesLock.Unlock()
	schema, found := e.tables[tableName]
	if !found {
		err := e.createTable(ctx, tableName, e.sysCols.Merge(dataset.Schema.Columns()))
		if err != nil {
			return span.Err(err)
		}
		e.tables[tableName] = dataset.Schema
	} else {
		alterSchema, failures := schema.GetAlterSchema(dataset)
		if len(failures) > 0 {
			e.writeErrSpans(ctx, failures)
			return span.Err(
				errors.Errorf(
					"alter table failed (%v failures), first failure: %v", len(failures), failures[0].Err(),
				),
			)
		}
		if alterSchema.IsZero() {
			return nil
		}
		if err := e.alterTable(ctx, tableName, alterSchema); err != nil {
			return span.Err(err)
		}
		e.tables[tableName] = e.tables[tableName].Merge(alterSchema.Columns())
	}
	return nil
}

func (e *Exporter) writeRecordSet(ctx context.Context, tableName string, recordSet RecordSet) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.WriteTimeout)
	defer cancel()
	span, ctx := klogga.Start(ctx)
	defer func() {
		e.writeIfErr(span)
	}()
	span.Tag("table", tableName)

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToConnectPG))
		return
	}
	defer func() { span.DeferErr(conn.Close()) }()

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

	query := pq.CopyInSchema(e.cfg.SchemaName, tableName, recordSet.Schema.ColumnNames()...)
	span.Val(vals.Query, query)
	span.Val("columns_count", recordSet.Schema.ColumnsCount())
	stmt, err := tx.Prepare(query)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToPrepareStatement))
		return
	}
	defer func() {
		span.DeferErr(errors.Wrap(stmt.Close(), "unable to close statement"))
	}()

	for _, span := range recordSet.Spans {
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
			span.ID().Bytes(),
			span.TraceID().AsUUID(),
			span.Host(),
			span.PackageClass(),
			span.Name(),
			span.ParentID().AsNullableBytes(),
			strErr,
			strWarn,
			span.Duration(),
		}

		for _, colName := range recordSet.Schema.ColumnNames()[e.sysCols.ColumnsCount():] {
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

	if _, err = stmt.ExecContext(ctx); err != nil {
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
	defer e.trs.Finish(span)

	q := schema.CreateTableStatement(e.cfg.SchemaName, tableName, e.timeCol, e.cfg.UseTimescale)
	span.
		Tag("table", tableName).
		Val("columns_count", schema.ColumnsCount()).
		Val("columns", strings.Join(schema.ColumnNames(), ",")).
		Val(vals.Query, q)

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToConnectPG))
	}
	defer func() { span.DeferErr(conn.Close()) }()
	if _, err := conn.ExecContext(ctx, q); err != nil {
		span.Val(vals.Query, q)
		return span.Err(errors.Wrap(err, msgUnableToExec))
	}
	return nil
}

func (e *Exporter) alterTable(ctx context.Context, tableName string, schema *TableSchema) error {
	span, ctx := klogga.Start(ctx)
	defer e.trs.Finish(span)
	if schema.IsZero() {
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
	defer func() { span.DeferErr(conn.Close()) }()
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return span.Err(errors.Wrap(err, msgUnableToExec))
	}
	return nil
}

// loadSchemas loads schema to cache from postgres
func (e *Exporter) loadSchemas(ctx context.Context) error {
	span, ctx := klogga.Start(ctx)
	defer e.trs.Finish(span)
	span.Val("timeout", e.cfg.LoadSchemaTimeout)
	ctx, cancel := context.WithTimeout(ctx, e.cfg.LoadSchemaTimeout)
	defer cancel()
	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		return span.Err(errors.Wrap(err, msgUnableToConnectPG))
	}
	defer func() { span.DeferErr(conn.Close()) }()

	query, args := e.psql.Select("table_name", "column_name", "data_type").
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

	tables := map[string]*TableSchema{}
	for rows.Next() {
		var tableName string
		var colSchema ColumnSchema
		if err := rows.Scan(&tableName, &colSchema.Name, &colSchema.DataType); err != nil {
			return span.Err(errors.Wrap(err, magUnableToScan))
		}
		if _, found := tables[tableName]; !found {
			tables[tableName] = NewTableSchema([]*ColumnSchema{})
		}
		tables[tableName].AddColumn(colSchema)
	}
	_, ok := tables[ErrorPostgresTableName]
	if !ok {
		err := e.createTable(ctx, ErrorPostgresTableName, e.errTable)
		if err != nil {
			span.ErrVoid(errors.Wrapf(err, "failed to create table for errors: %s", ErrorPostgresTable))
		}
	}

	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	span.Val("tables", strings.Join(names, ","))
	span.Val("count", len(tables))
	e.tables = tables
	return nil
}

func (e *Exporter) writeErrSpans(ctx context.Context, errDescrs []ErrDescriptor) {
	if len(errDescrs) == 0 {
		return
	}
	span, ctx := klogga.Start(ctx)
	defer e.writeIfErr(span)
	span.Val(vals.Count, len(errDescrs))

	conn, err := e.connFactory.GetConnection(ctx)
	if err != nil {
		span.ErrVoid(errors.Wrap(err, msgUnableToConnectPG))
		return
	}
	defer func() { span.DeferErr(conn.Close()) }()

	statementText := e.errTable.InsertStatement(e.cfg.SchemaName, ErrorPostgresTableName)

	for i := 0; i < len(errDescrs); i++ {
		errDescr := errDescrs[i]
		warn := ""
		if errDescr.Warn() != nil {
			warn = errDescr.Warn().Error()
		}
		_, err := conn.ExecContext(
			ctx,
			statementText,
			errDescr.Span.StartedTs(),
			errDescr.Span.ID().Bytes(),
			errDescr.Span.TraceID().AsUUID(),
			errDescr.Span.Host(),
			errDescr.Span.PackageClass(),
			errDescr.Span.Name(),
			errDescr.Span.ParentID().AsNullableBytes(),
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

type RecordSet struct {
	Schema *TableSchema
	Spans  []*klogga.Span
}

// createRecordSets creates record sets from spans, grouped by table name
// if span column can't be placed in the structure, it is added to []ErrDescriptor
// (this happens when span has a data type in tag/val that is different from the expected type
// in the existing structure)
func (e *Exporter) createRecordSets(spans ...*klogga.Span) (map[string]RecordSet, []ErrDescriptor) {
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
				errSpans = append(errSpans, newErrDescriptor(tableName, span, newColumn, *col))
			}
		}

		if len(errSpans) == 0 {
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
	table = stringutil.ToSnakeCase(table)
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

func (e *Exporter) writeIfErr(span *klogga.Span) {
	if span.HasErr() {
		span.FlushTo(e.trs)
	}
}
