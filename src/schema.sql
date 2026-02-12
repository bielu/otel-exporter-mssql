-- MSSQL Schema for OpenTelemetry Collector Exporter

CREATE TABLE [Resources] (
  [ResourceId] bigint PRIMARY KEY IDENTITY(1, 1),
  [ServiceName] nvarchar(200) NOT NULL
)
GO

CREATE TABLE [ResourceAttributes] (
  [ResourceId] bigint NOT NULL,
  [AttrKey] nvarchar(200) NOT NULL,
  [StringValue] nvarchar(max),
  [IntValue] bigint,
  [DoubleValue] float,
  [BoolValue] bit,
  PRIMARY KEY ([ResourceId], [AttrKey])
)
GO

CREATE TABLE [Spans] (
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
)
GO

CREATE TABLE [SpanAttributes] (
  [TraceId] varbinary(16) NOT NULL,
  [SpanId] varbinary(8) NOT NULL,
  [AttrKey] nvarchar(200) NOT NULL,
  [StringValue] nvarchar(max),
  [IntValue] bigint,
  [DoubleValue] float,
  [BoolValue] bit,
  PRIMARY KEY ([TraceId], [SpanId], [AttrKey])
)
GO

CREATE TABLE [SpanEvents] (
  [EventId] bigint PRIMARY KEY IDENTITY(1, 1),
  [TraceId] varbinary(16) NOT NULL,
  [SpanId] varbinary(8) NOT NULL,
  [EventName] nvarchar(200) NOT NULL,
  [EventTime] datetime2(7) NOT NULL
)
GO

CREATE TABLE [SpanEventAttributes] (
  [EventId] bigint NOT NULL,
  [AttrKey] nvarchar(200) NOT NULL,
  [StringValue] nvarchar(max),
  [IntValue] bigint,
  [DoubleValue] float,
  [BoolValue] bit,
  PRIMARY KEY ([EventId], [AttrKey])
)
GO

CREATE TABLE [Logs] (
  [LogId] bigint PRIMARY KEY IDENTITY(1, 1),
  [ResourceId] bigint NOT NULL,
  [TraceId] varbinary(16),
  [SpanId] varbinary(8),
  [Timestamp] datetime2(7) NOT NULL,
  [SeverityNumber] tinyint,
  [SeverityText] nvarchar(50),
  [Body] nvarchar(max)
)
GO

CREATE TABLE [LogAttributes] (
  [LogId] bigint NOT NULL,
  [AttrKey] nvarchar(200) NOT NULL,
  [StringValue] nvarchar(max),
  [IntValue] bigint,
  [DoubleValue] float,
  [BoolValue] bit,
  PRIMARY KEY ([LogId], [AttrKey])
)
GO

CREATE TABLE [Metrics] (
  [MetricId] bigint PRIMARY KEY IDENTITY(1, 1),
  [ResourceId] bigint NOT NULL,
  [MetricName] nvarchar(200) NOT NULL,
  [MetricType] tinyint NOT NULL,
  [Unit] nvarchar(50),
  [Description] nvarchar(1000)
)
GO

CREATE TABLE [MetricDataPoints] (
  [DataPointId] bigint PRIMARY KEY IDENTITY(1, 1),
  [MetricId] bigint NOT NULL,
  [Timestamp] datetime2(7) NOT NULL,
  [ValueDouble] float,
  [ValueLong] bigint,
  [Count] bigint,
  [Sum] float
)
GO

CREATE TABLE [MetricDataPointAttributes] (
  [DataPointId] bigint NOT NULL,
  [AttrKey] nvarchar(200) NOT NULL,
  [StringValue] nvarchar(max),
  [IntValue] bigint,
  [DoubleValue] float,
  [BoolValue] bit,
  PRIMARY KEY ([DataPointId], [AttrKey])
)
GO

CREATE INDEX [ResourceAttributes_index_0] ON [ResourceAttributes] ("AttrKey")
GO

CREATE INDEX [Spans_index_1] ON [Spans] ("ResourceId", "StartTime")
GO

CREATE INDEX [Spans_index_2] ON [Spans] ("DurationNs")
GO

CREATE INDEX [Spans_index_3] ON [Spans] ("StatusCode")
GO

CREATE INDEX [SpanAttributes_index_4] ON [SpanAttributes] ("AttrKey")
GO

CREATE INDEX [SpanEventAttributes_index_5] ON [SpanEventAttributes] ("AttrKey")
GO

CREATE INDEX [Logs_index_6] ON [Logs] ("ResourceId", "Timestamp")
GO

CREATE INDEX [Logs_index_7] ON [Logs] ("TraceId")
GO

CREATE INDEX [Logs_index_8] ON [Logs] ("SeverityNumber")
GO

CREATE INDEX [LogAttributes_index_9] ON [LogAttributes] ("AttrKey")
GO

CREATE INDEX [Metrics_index_10] ON [Metrics] ("ResourceId", "MetricName")
GO

CREATE INDEX [MetricDataPointAttributes_index_11] ON [MetricDataPointAttributes] ("AttrKey")
GO

ALTER TABLE [ResourceAttributes] ADD FOREIGN KEY ([ResourceId]) REFERENCES [Resources] ([ResourceId])
GO

ALTER TABLE [Spans] ADD FOREIGN KEY ([ResourceId]) REFERENCES [Resources] ([ResourceId])
GO

ALTER TABLE [SpanAttributes] ADD FOREIGN KEY ([TraceId], [SpanId]) REFERENCES [Spans] ([TraceId], [SpanId])
GO

ALTER TABLE [SpanEvents] ADD FOREIGN KEY ([TraceId], [SpanId]) REFERENCES [Spans] ([TraceId], [SpanId])
GO

ALTER TABLE [SpanEventAttributes] ADD FOREIGN KEY ([EventId]) REFERENCES [SpanEvents] ([EventId])
GO

ALTER TABLE [Logs] ADD FOREIGN KEY ([ResourceId]) REFERENCES [Resources] ([ResourceId])
GO

ALTER TABLE [LogAttributes] ADD FOREIGN KEY ([LogId]) REFERENCES [Logs] ([LogId])
GO

ALTER TABLE [Metrics] ADD FOREIGN KEY ([ResourceId]) REFERENCES [Resources] ([ResourceId])
GO

ALTER TABLE [MetricDataPoints] ADD FOREIGN KEY ([MetricId]) REFERENCES [Metrics] ([MetricId])
GO

ALTER TABLE [MetricDataPointAttributes] ADD FOREIGN KEY ([DataPointId]) REFERENCES [MetricDataPoints] ([DataPointId])
GO
