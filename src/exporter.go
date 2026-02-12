// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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
	db, err := sql.Open("sqlserver", e.cfg.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	e.db = db
	e.logger.Info("Connected to MSSQL database")
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
		"INSERT INTO Resources (ServiceName) OUTPUT INSERTED.ResourceId VALUES (@p1)",
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
		`INSERT INTO ResourceAttributes (ResourceId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
		 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			b := parentSpanID
			parentSpanIDBytes = b[:]
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
			`INSERT INTO Spans (TraceId, SpanId, ParentSpanId, ResourceId, SpanName, SpanKind, StartTime, EndTime, DurationNs, StatusCode, StatusMessage, TraceState)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12)`,
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
			`INSERT INTO SpanAttributes (TraceId, SpanId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`,
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
			`INSERT INTO SpanEvents (TraceId, SpanId, EventName, EventTime) OUTPUT INSERTED.EventId VALUES (@p1, @p2, @p3, @p4)`,
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
			`INSERT INTO SpanEventAttributes (EventId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			`INSERT INTO Logs (ResourceId, TraceId, SpanId, Timestamp, SeverityNumber, SeverityText, Body)
			 OUTPUT INSERTED.LogId VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`,
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
			`INSERT INTO LogAttributes (LogId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			`INSERT INTO Metrics (ResourceId, MetricName, MetricType, Unit, Description)
			 OUTPUT INSERTED.MetricId VALUES (@p1, @p2, @p3, @p4, @p5)`,
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
			`INSERT INTO MetricDataPoints (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			`INSERT INTO MetricDataPoints (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			`INSERT INTO MetricDataPoints (MetricId, Timestamp, ValueDouble, ValueLong, Count, Sum)
			 OUTPUT INSERTED.DataPointId VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
			`INSERT INTO MetricDataPointAttributes (DataPointId, AttrKey, StringValue, IntValue, DoubleValue, BoolValue)
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
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
