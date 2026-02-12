# MSSQL Exporter for OpenTelemetry Collector

This exporter allows the OpenTelemetry Collector to export telemetry data (traces, logs, and metrics) to a Microsoft SQL Server database.

## Configuration

```yaml
exporters:
  mssql:
    # Required: Connection string for the MSSQL database
    connection_string: "sqlserver://username:password@host:1433?database=otel"
    
    # Optional: Maximum number of records to insert in a single batch
    # Default: 1000
    batch_size: 1000
    
    # Optional: Retry settings for failed exports
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
```

## Database Schema

The exporter requires the following database schema to be created in your MSSQL database. You can find the complete schema in [schema.sql](schema.sql).

### Tables

#### Resources
- `Resources` - Stores resource information (service name)
- `ResourceAttributes` - Stores resource attributes

#### Traces
- `Spans` - Stores span information
- `SpanAttributes` - Stores span attributes
- `SpanEvents` - Stores span events
- `SpanEventAttributes` - Stores span event attributes

#### Logs
- `Logs` - Stores log records
- `LogAttributes` - Stores log attributes

#### Metrics
- `Metrics` - Stores metric metadata
- `MetricDataPoints` - Stores metric data points
- `MetricDataPointAttributes` - Stores data point attributes

## Features

- **Batch Processing**: All telemetry data is inserted in batches for improved performance
- **Transaction Support**: Each push operation is wrapped in a database transaction for data consistency
- **Retry Support**: Built-in retry mechanism for handling transient failures
- **Full Telemetry Support**: Exports traces, logs, and metrics

## Supported Metric Types

- Gauge
- Sum
- Histogram
- Summary
- Exponential Histogram

## Example Pipeline Configuration

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:

exporters:
  mssql:
    connection_string: "sqlserver://sa:YourStrong@Passw0rd@localhost:1433?database=otel"
    batch_size: 1000

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [mssql]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [mssql]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [mssql]
```

## Development

### Building

```bash
cd src
go build ./...
```

### Testing

```bash
cd src
go test ./...
```

## License

Apache-2.0
