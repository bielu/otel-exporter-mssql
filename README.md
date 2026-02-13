# MSSQL Exporter for OpenTelemetry Collector

This exporter allows the OpenTelemetry Collector to export telemetry data (traces, logs, and metrics) to a Microsoft SQL Server database.

## Configuration

```yaml
exporters:
  mssql:
    # Required: Connection string for the MSSQL database
    connection_string: "sqlserver://username:password@host:1433"
    
    # Optional: Database name (can also be specified in connection_string)
    database: "otel"
    
    # Optional: Prefix for all table names
    # If set to "otel_", tables will be named "otel_Resources", "otel_Spans", etc.
    table_prefix: ""
    
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

The exporter automatically creates all required tables and indexes on startup if they don't exist. This means you don't need to manually set up the database schema - just ensure the database exists and the connection has sufficient permissions to create tables.

If you prefer to create the schema manually, you can find the complete schema in [schema.sql](src/schema.sql). When using a `table_prefix`, modify the table names in the schema accordingly.

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

- **Automatic Table Creation**: Tables and indexes are automatically created on startup if they don't exist
- **Batch Processing**: All telemetry data is inserted in batches for improved performance
- **Transaction Support**: Each push operation is wrapped in a database transaction for data consistency
- **Retry Support**: Built-in retry mechanism for handling transient failures
- **Full Telemetry Support**: Exports traces, logs, and metrics
- **Table Prefix Support**: Configure custom table name prefixes for multi-tenant deployments
- **Flexible Database Configuration**: Specify database separately or in connection string

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
    connection_string: "sqlserver://sa:YourStrong@Passw0rd@localhost:1433"
    database: "otel"
    table_prefix: "otel_"
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

## Docker

This repository includes a custom OpenTelemetry Collector Docker image with the MSSQL exporter pre-built.

### Quick Start with Docker Compose

The easiest way to get started is using Docker Compose, which sets up both MSSQL Server and the OpenTelemetry Collector:

```bash
docker compose up -d
```

This will:
1. Start a Microsoft SQL Server 2025 instance
2. Build and start the OpenTelemetry Collector with the MSSQL exporter

> **Note**: You need to create the `otel` database before the exporter can store data. Connect to SQL Server and run: `CREATE DATABASE otel`

The collector will be available at:
- **OTLP gRPC**: `localhost:4317`
- **OTLP HTTP**: `localhost:4318`
- **zPages**: `localhost:55679`

### Building the Docker Image

To build the custom collector image manually:

```bash
docker build -t otelcol-mssql .
```

### Running the Docker Image

```bash
docker run -v /path/to/your/config.yaml:/etc/otelcol/config.yaml otelcol-mssql
```

### Building a Custom Collector

The repository uses the OpenTelemetry Collector Builder (OCB). The build configuration is in `otelcol-builder.yaml`. You can customize the included components by editing this file.

To build locally (requires Go and OCB installed):

```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.145.0
builder --config=otelcol-builder.yaml
./otelcol-mssql/otelcol-mssql --config=otelcol-config.yaml
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

MIT
