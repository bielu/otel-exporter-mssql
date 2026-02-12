// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: MIT

package mssqlexporter

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// mssqlExporter is the MSSQL exporter implementation.
type mssqlExporter struct {
	cfg    *Config
	logger *zap.Logger
	db     *sql.DB
}

// newMSSQLExporter creates a new MSSQL exporter.
func newMSSQLExporter(cfg *Config, logger *zap.Logger) (*mssqlExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &mssqlExporter{
		cfg:    cfg,
		logger: logger,
	}, nil
}

// start opens the database connection.
func (e *mssqlExporter) start(ctx context.Context, _ component.Host) error {
	db, err := sql.Open("sqlserver", e.cfg.GetConnectionString())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	e.db = db
	e.logger.Info("Connected to MSSQL database")

	// Create tables if they don't exist
	if err := e.ensureTablesExist(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to create tables: %w", err)
	}

	return nil
}

// ensureTablesExist creates all required tables if they don't exist.
func (e *mssqlExporter) ensureTablesExist(ctx context.Context) error {
	e.logger.Info("Ensuring tables exist", zap.String("table_prefix", e.cfg.TablePrefix))

	// Table definitions with their names for better error reporting
	tableDefinitions := []struct {
		name string
		sql  string
	}{
		{
			name: "Resources",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[ResourceId] bigint PRIMARY KEY IDENTITY(1, 1),
				[ServiceName] nvarchar(200) NOT NULL
			)`,
		},
		{
			name: "ResourceAttributes",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[ResourceId] bigint NOT NULL,
				[AttrKey] nvarchar(200) NOT NULL,
				[StringValue] nvarchar(max),
				[IntValue] bigint,
				[DoubleValue] float,
				[BoolValue] bit,
				PRIMARY KEY ([ResourceId], [AttrKey])
			)`,
		},
		{
			name: "Spans",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[TraceId] varbinary(16) NOT NULL,
				[SpanId] varbinary(8) NOT NULL,
				[ParentSpanId] varbinary(8),
				[ResourceId] bigint NOT NULL,
				[SpanName] nvarchar(500) NOT NULL,
				[SpanKind] tinyint NOT NULL,
				[StartTime] datetime2(7) NOT NULL,
				[EndTime] datetime2(7) NOT NULL,
				[DurationNs] bigint NOT NULL,
				[StatusCode] tinyint,
				[StatusMessage] nvarchar(1000),
				[TraceState] nvarchar(500),
				PRIMARY KEY ([TraceId], [SpanId])
			)`,
		},
		{
			name: "SpanAttributes",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[TraceId] varbinary(16) NOT NULL,
				[SpanId] varbinary(8) NOT NULL,
				[AttrKey] nvarchar(200) NOT NULL,
				[StringValue] nvarchar(max),
				[IntValue] bigint,
				[DoubleValue] float,
				[BoolValue] bit,
				PRIMARY KEY ([TraceId], [SpanId], [AttrKey])
			)`,
		},
		{
			name: "SpanEvents",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[EventId] bigint PRIMARY KEY IDENTITY(1, 1),
				[TraceId] varbinary(16) NOT NULL,
				[SpanId] varbinary(8) NOT NULL,
				[EventName] nvarchar(200) NOT NULL,
				[EventTime] datetime2(7) NOT NULL
			)`,
		},
		{
			name: "SpanEventAttributes",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[EventId] bigint NOT NULL,
				[AttrKey] nvarchar(200) NOT NULL,
				[StringValue] nvarchar(max),
				[IntValue] bigint,
				[DoubleValue] float,
				[BoolValue] bit,
				PRIMARY KEY ([EventId], [AttrKey])
			)`,
		},
		{
			name: "Logs",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[LogId] bigint PRIMARY KEY IDENTITY(1, 1),
				[ResourceId] bigint NOT NULL,
				[TraceId] varbinary(16),
				[SpanId] varbinary(8),
				[Timestamp] datetime2(7) NOT NULL,
				[SeverityNumber] tinyint,
				[SeverityText] nvarchar(50),
				[Body] nvarchar(max)
			)`,
		},
		{
			name: "LogAttributes",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[LogId] bigint NOT NULL,
				[AttrKey] nvarchar(200) NOT NULL,
				[StringValue] nvarchar(max),
				[IntValue] bigint,
				[DoubleValue] float,
				[BoolValue] bit,
				PRIMARY KEY ([LogId], [AttrKey])
			)`,
		},
		{
			name: "Metrics",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[MetricId] bigint PRIMARY KEY IDENTITY(1, 1),
				[ResourceId] bigint NOT NULL,
				[MetricName] nvarchar(200) NOT NULL,
				[MetricType] tinyint NOT NULL,
				[Unit] nvarchar(50),
				[Description] nvarchar(1000)
			)`,
		},
		{
			name: "MetricDataPoints",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[DataPointId] bigint PRIMARY KEY IDENTITY(1, 1),
				[MetricId] bigint NOT NULL,
				[Timestamp] datetime2(7) NOT NULL,
				[ValueDouble] float,
				[ValueLong] bigint,
				[Count] bigint,
				[Sum] float
			)`,
		},
		{
			name: "MetricDataPointAttributes",
			sql: `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @p1)
			CREATE TABLE [%s] (
				[DataPointId] bigint NOT NULL,
				[AttrKey] nvarchar(200) NOT NULL,
				[StringValue] nvarchar(max),
				[IntValue] bigint,
				[DoubleValue] float,
				[BoolValue] bit,
				PRIMARY KEY ([DataPointId], [AttrKey])
			)`,
		},
	}

	// Execute table creation statements
	for _, table := range tableDefinitions {
		tableName := e.cfg.TableName(table.name)
		stmt := fmt.Sprintf(table.sql, tableName)
		if _, err := e.db.ExecContext(ctx, stmt, tableName); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	// Create indexes - using simple index creation that checks existence
	indexDefinitions := []struct {
		tableName string
		indexName string
		columns   string
	}{
		{"ResourceAttributes", "AttrKey", "[AttrKey]"},
		{"Spans", "ResourceId_StartTime", "[ResourceId], [StartTime]"},
		{"Spans", "DurationNs", "[DurationNs]"},
		{"Spans", "StatusCode", "[StatusCode]"},
		{"SpanAttributes", "AttrKey", "[AttrKey]"},
		{"SpanEventAttributes", "AttrKey", "[AttrKey]"},
		{"Logs", "ResourceId_Timestamp", "[ResourceId], [Timestamp]"},
		{"Logs", "TraceId", "[TraceId]"},
		{"Logs", "SeverityNumber", "[SeverityNumber]"},
		{"LogAttributes", "AttrKey", "[AttrKey]"},
		{"Metrics", "ResourceId_MetricName", "[ResourceId], [MetricName]"},
		{"MetricDataPointAttributes", "AttrKey", "[AttrKey]"},
	}

	for _, idx := range indexDefinitions {
		tableName := e.cfg.TableName(idx.tableName)
		indexName := fmt.Sprintf("idx_%s_%s", tableName, idx.indexName)
		stmt := fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = @p1 AND object_id = OBJECT_ID(@p2))
			CREATE INDEX [%s] ON [%s] (%s)`, indexName, tableName, idx.columns)
		if _, err := e.db.ExecContext(ctx, stmt, indexName, tableName); err != nil {
			e.logger.Warn("Failed to create index", zap.String("index", indexName), zap.Error(err))
			// Continue even if index creation fails
		}
	}

	e.logger.Info("Tables verified/created successfully")
	return nil
}

// shutdown closes the database connection.
func (e *mssqlExporter) shutdown(_ context.Context) error {
	if e.db != nil {
		e.logger.Info("Closing MSSQL database connection")
		return e.db.Close()
	}
	return nil
}

// pushTraces exports traces to MSSQL.
func (e *mssqlExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	resourceSpans := td.ResourceSpans()
	if resourceSpans.Len() == 0 {
		return nil
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resourceID, err := e.insertResource(ctx, tx, rs.Resource())
		if err != nil {
			return fmt.Errorf("failed to insert resource: %w", err)
		}

		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			spans := scopeSpans.At(j).Spans()
			if err := e.insertSpans(ctx, tx, resourceID, spans); err != nil {
				return fmt.Errorf("failed to insert spans: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// pushLogs exports logs to MSSQL.
func (e *mssqlExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	resourceLogs := ld.ResourceLogs()
	if resourceLogs.Len() == 0 {
		return nil
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		resourceID, err := e.insertResource(ctx, tx, rl.Resource())
		if err != nil {
			return fmt.Errorf("failed to insert resource: %w", err)
		}

		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			logs := scopeLogs.At(j).LogRecords()
			if err := e.insertLogs(ctx, tx, resourceID, logs); err != nil {
				return fmt.Errorf("failed to insert logs: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// pushMetrics exports metrics to MSSQL.
func (e *mssqlExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	resourceMetrics := md.ResourceMetrics()
	if resourceMetrics.Len() == 0 {
		return nil
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		resourceID, err := e.insertResource(ctx, tx, rm.Resource())
		if err != nil {
			return fmt.Errorf("failed to insert resource: %w", err)
		}

		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			metrics := scopeMetrics.At(j).Metrics()
			if err := e.insertMetrics(ctx, tx, resourceID, metrics); err != nil {
				return fmt.Errorf("failed to insert metrics: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// insertResource inserts a resource and its attributes into the database.
func (e *mssqlExporter) insertResource(ctx context.Context, tx *sql.Tx, resource pcommon.Resource) (int64, error) {
	attrs := resource.Attributes()
	serviceName := "unknown"
	if sn, ok := attrs.Get("service.name"); ok {
		serviceName = sn.AsString()
	}

	var resourceID int64
	err := tx.QueryRowContext(ctx,
		fmt.Sprintf("INSERT INTO %s (ServiceName) OUTPUT INSERTED.ResourceId VALUES (@p1)", e.cfg.TableName("Resources")),
		serviceName,
	).Scan(&resourceID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert resource: %w", err)
	}

	// Insert resource attributes in batches
	attrCount := 0
	attrs.Range(func(key string, value pcommon.Value) bool {
		if attrCount >= e.cfg.BatchSize {
			return false
		}
		if err := e.insertResourceAttribute(ctx, tx, resourceID, key, value); err != nil {
			e.logger.Error("Failed to insert resource attribute", zap.Error(err))
		}
		attrCount++
		return true
	})

	return resourceID, nil
}

// insertResourceAttribute inserts a single resource attribute.
func (e *mssqlExporter) insertResourceAttribute(ctx context.Context, tx *sql.Tx, resourceID int64, key string, value pcommon.Value) error {
	stringVal, intVal, doubleVal, boolVal := extractAttributeValues(value)

	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (ResourceId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
		 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("ResourceAttributes")),
		resourceID, key, stringVal, intVal, doubleVal, boolVal,
	)
	return err
}

// insertSpans inserts spans and their attributes, events in batches.
func (e *mssqlExporter) insertSpans(ctx context.Context, tx *sql.Tx, resourceID int64, spans ptrace.SpanSlice) error {
	for i := 0; i < spans.Len(); i++ {
		span := spans.At(i)

		traceID := span.TraceID()
		spanID := span.SpanID()
		parentSpanID := span.ParentSpanID()

		var parentSpanIDBytes interface{}
		if !parentSpanID.IsEmpty() {
			parentID := parentSpanID
			parentSpanIDBytes = parentID[:]
		}

		startTime := span.StartTimestamp().AsTime()
		endTime := span.EndTimestamp().AsTime()
		durationNs := int64(span.EndTimestamp()) - int64(span.StartTimestamp())

		var statusMessage interface{}
		if msg := span.Status().Message(); msg != "" {
			statusMessage = msg
		}

		var traceState interface{}
		if ts := span.TraceState().AsRaw(); ts != "" {
			traceState = ts
		}

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (TraceId, SpanId, ParentSpanId, ResourceId, SpanName, SpanKind, StartTime, EndTime, DurationNs, StatusCode, StatusMessage, TraceState)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12)`, e.cfg.TableName("Spans")),
			traceID[:], spanID[:], parentSpanIDBytes, resourceID, span.Name(), int(span.Kind()),
			startTime, endTime, durationNs, int(span.Status().Code()), statusMessage, traceState,
		)
		if err != nil {
			return fmt.Errorf("failed to insert span: %w", err)
		}

		// Insert span attributes
		if err := e.insertSpanAttributes(ctx, tx, traceID, spanID, span.Attributes()); err != nil {
			return fmt.Errorf("failed to insert span attributes: %w", err)
		}

		// Insert span events
		if err := e.insertSpanEvents(ctx, tx, traceID, spanID, span.Events()); err != nil {
			return fmt.Errorf("failed to insert span events: %w", err)
		}
	}

	return nil
}

// insertSpanAttributes inserts span attributes.
func (e *mssqlExporter) insertSpanAttributes(ctx context.Context, tx *sql.Tx, traceID pcommon.TraceID, spanID pcommon.SpanID, attrs pcommon.Map) error {
	attrCount := 0
	var lastErr error
	attrs.Range(func(key string, value pcommon.Value) bool {
		if attrCount >= e.cfg.BatchSize {
			return false
		}
		stringVal, intVal, doubleVal, boolVal := extractAttributeValues(value)

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (TraceId, SpanId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`, e.cfg.TableName("SpanAttributes")),
			traceID[:], spanID[:], key, stringVal, intVal, doubleVal, boolVal,
		)
		if err != nil {
			lastErr = err
			return false
		}
		attrCount++
		return true
	})
	return lastErr
}

// insertSpanEvents inserts span events and their attributes.
func (e *mssqlExporter) insertSpanEvents(ctx context.Context, tx *sql.Tx, traceID pcommon.TraceID, spanID pcommon.SpanID, events ptrace.SpanEventSlice) error {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)

		var eventID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (TraceId, SpanId, EventName, EventTime) OUTPUT INSERTED.EventId VALUES (@p1, @p2, @p3, @p4)`, e.cfg.TableName("SpanEvents")),
			traceID[:], spanID[:], event.Name(), event.Timestamp().AsTime(),
		).Scan(&eventID)
		if err != nil {
			return fmt.Errorf("failed to insert span event: %w", err)
		}

		// Insert event attributes
		if err := e.insertSpanEventAttributes(ctx, tx, eventID, event.Attributes()); err != nil {
			return fmt.Errorf("failed to insert span event attributes: %w", err)
		}
	}
	return nil
}

// insertSpanEventAttributes inserts span event attributes.
func (e *mssqlExporter) insertSpanEventAttributes(ctx context.Context, tx *sql.Tx, eventID int64, attrs pcommon.Map) error {
	attrCount := 0
	var lastErr error
	attrs.Range(func(key string, value pcommon.Value) bool {
		if attrCount >= e.cfg.BatchSize {
			return false
		}
		stringVal, intVal, doubleVal, boolVal := extractAttributeValues(value)

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (EventId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("SpanEventAttributes")),
			eventID, key, stringVal, intVal, doubleVal, boolVal,
		)
		if err != nil {
			lastErr = err
			return false
		}
		attrCount++
		return true
	})
	return lastErr
}

// insertLogs inserts log records and their attributes.
func (e *mssqlExporter) insertLogs(ctx context.Context, tx *sql.Tx, resourceID int64, logs plog.LogRecordSlice) error {
	for i := 0; i < logs.Len(); i++ {
		logRecord := logs.At(i)

		var traceIDBytes, spanIDBytes interface{}
		if !logRecord.TraceID().IsEmpty() {
			tid := logRecord.TraceID()
			traceIDBytes = tid[:]
		}
		if !logRecord.SpanID().IsEmpty() {
			sid := logRecord.SpanID()
			spanIDBytes = sid[:]
		}

		timestamp := logRecord.Timestamp().AsTime()
		if timestamp.IsZero() {
			timestamp = time.Now()
		}

		var severityText interface{}
		if st := logRecord.SeverityText(); st != "" {
			severityText = st
		}

		body := logRecord.Body().AsString()

		var logID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (ResourceId, TraceId, SpanId, Timestamp, SeverityNumber, SeverityText, Body)
			 OUTPUT INSERTED.LogId VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`, e.cfg.TableName("Logs")),
			resourceID, traceIDBytes, spanIDBytes, timestamp, int(logRecord.SeverityNumber()), severityText, body,
		).Scan(&logID)
		if err != nil {
			return fmt.Errorf("failed to insert log: %w", err)
		}

		// Insert log attributes
		if err := e.insertLogAttributes(ctx, tx, logID, logRecord.Attributes()); err != nil {
			return fmt.Errorf("failed to insert log attributes: %w", err)
		}
	}

	return nil
}

// insertLogAttributes inserts log attributes.
func (e *mssqlExporter) insertLogAttributes(ctx context.Context, tx *sql.Tx, logID int64, attrs pcommon.Map) error {
	attrCount := 0
	var lastErr error
	attrs.Range(func(key string, value pcommon.Value) bool {
		if attrCount >= e.cfg.BatchSize {
			return false
		}
		stringVal, intVal, doubleVal, boolVal := extractAttributeValues(value)

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (LogId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("LogAttributes")),
			logID, key, stringVal, intVal, doubleVal, boolVal,
		)
		if err != nil {
			lastErr = err
			return false
		}
		attrCount++
		return true
	})
	return lastErr
}

// insertMetrics inserts metrics and their data points.
func (e *mssqlExporter) insertMetrics(ctx context.Context, tx *sql.Tx, resourceID int64, metrics pmetric.MetricSlice) error {
	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)

		var unit, description interface{}
		if u := metric.Unit(); u != "" {
			unit = u
		}
		if d := metric.Description(); d != "" {
			description = d
		}

		metricType := getMetricType(metric)

		var metricID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (ResourceId, MetricName, MetricType, Unit, Description)
			 OUTPUT INSERTED.MetricId VALUES (@p1, @p2, @p3, @p4, @p5)`, e.cfg.TableName("Metrics")),
			resourceID, metric.Name(), metricType, unit, description,
		).Scan(&metricID)
		if err != nil {
			return fmt.Errorf("failed to insert metric: %w", err)
		}

		// Insert data points based on metric type
		if err := e.insertMetricDataPoints(ctx, tx, metricID, metric); err != nil {
			return fmt.Errorf("failed to insert metric data points: %w", err)
		}
	}

	return nil
}

// getMetricType returns the type identifier for a metric.
func getMetricType(metric pmetric.Metric) int {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return 1
	case pmetric.MetricTypeSum:
		return 2
	case pmetric.MetricTypeHistogram:
		return 3
	case pmetric.MetricTypeExponentialHistogram:
		return 4
	case pmetric.MetricTypeSummary:
		return 5
	default:
		return 0
	}
}

// insertMetricDataPoints inserts metric data points.
func (e *mssqlExporter) insertMetricDataPoints(ctx context.Context, tx *sql.Tx, metricID int64, metric pmetric.Metric) error {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return e.insertGaugeDataPoints(ctx, tx, metricID, metric.Gauge().DataPoints())
	case pmetric.MetricTypeSum:
		return e.insertNumberDataPoints(ctx, tx, metricID, metric.Sum().DataPoints())
	case pmetric.MetricTypeHistogram:
		return e.insertHistogramDataPoints(ctx, tx, metricID, metric.Histogram().DataPoints())
	case pmetric.MetricTypeSummary:
		return e.insertSummaryDataPoints(ctx, tx, metricID, metric.Summary().DataPoints())
	}
	return nil
}

// insertGaugeDataPoints inserts gauge data points.
func (e *mssqlExporter) insertGaugeDataPoints(ctx context.Context, tx *sql.Tx, metricID int64, dataPoints pmetric.NumberDataPointSlice) error {
	return e.insertNumberDataPoints(ctx, tx, metricID, dataPoints)
}

// insertNumberDataPoints inserts number data points (gauge or sum).
func (e *mssqlExporter) insertNumberDataPoints(ctx context.Context, tx *sql.Tx, metricID int64, dataPoints pmetric.NumberDataPointSlice) error {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		var valueDouble, valueLong interface{}
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			valueDouble = dp.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			valueLong = dp.IntValue()
		}

		var dataPointID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("MetricDataPoints")),
			metricID, dp.Timestamp().AsTime(), valueDouble, valueLong, nil, nil,
		).Scan(&dataPointID)
		if err != nil {
			return fmt.Errorf("failed to insert data point: %w", err)
		}

		// Insert data point attributes
		if err := e.insertMetricDataPointAttributes(ctx, tx, dataPointID, dp.Attributes()); err != nil {
			return fmt.Errorf("failed to insert data point attributes: %w", err)
		}
	}
	return nil
}

// insertHistogramDataPoints inserts histogram data points.
func (e *mssqlExporter) insertHistogramDataPoints(ctx context.Context, tx *sql.Tx, metricID int64, dataPoints pmetric.HistogramDataPointSlice) error {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		var dataPointID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("MetricDataPoints")),
			metricID, dp.Timestamp().AsTime(), nil, nil, int64(dp.Count()), dp.Sum(),
		).Scan(&dataPointID)
		if err != nil {
			return fmt.Errorf("failed to insert histogram data point: %w", err)
		}

		// Insert data point attributes
		if err := e.insertMetricDataPointAttributes(ctx, tx, dataPointID, dp.Attributes()); err != nil {
			return fmt.Errorf("failed to insert data point attributes: %w", err)
		}
	}
	return nil
}

// insertSummaryDataPoints inserts summary data points.
func (e *mssqlExporter) insertSummaryDataPoints(ctx context.Context, tx *sql.Tx, metricID int64, dataPoints pmetric.SummaryDataPointSlice) error {
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		var dataPointID int64
		err := tx.QueryRowContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("MetricDataPoints")),
			metricID, dp.Timestamp().AsTime(), nil, nil, int64(dp.Count()), dp.Sum(),
		).Scan(&dataPointID)
		if err != nil {
			return fmt.Errorf("failed to insert summary data point: %w", err)
		}

		// Insert data point attributes
		if err := e.insertMetricDataPointAttributes(ctx, tx, dataPointID, dp.Attributes()); err != nil {
			return fmt.Errorf("failed to insert data point attributes: %w", err)
		}
	}
	return nil
}

// insertMetricDataPointAttributes inserts metric data point attributes.
func (e *mssqlExporter) insertMetricDataPointAttributes(ctx context.Context, tx *sql.Tx, dataPointID int64, attrs pcommon.Map) error {
	attrCount := 0
	var lastErr error
	attrs.Range(func(key string, value pcommon.Value) bool {
		if attrCount >= e.cfg.BatchSize {
			return false
		}
		stringVal, intVal, doubleVal, boolVal := extractAttributeValues(value)

		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (DataPointId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`, e.cfg.TableName("MetricDataPointAttributes")),
			dataPointID, key, stringVal, intVal, doubleVal, boolVal,
		)
		if err != nil {
			lastErr = err
			return false
		}
		attrCount++
		return true
	})
	return lastErr
}

// extractAttributeValues extracts typed values from a pcommon.Value.
func extractAttributeValues(value pcommon.Value) (stringVal, intVal, doubleVal, boolVal interface{}) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		stringVal = value.Str()
	case pcommon.ValueTypeInt:
		intVal = value.Int()
	case pcommon.ValueTypeDouble:
		doubleVal = value.Double()
	case pcommon.ValueTypeBool:
		boolVal = value.Bool()
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		stringVal = value.AsString()
	default:
		stringVal = value.AsString()
	}
	return
}
