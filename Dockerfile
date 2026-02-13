# Stage 1: Build the custom OpenTelemetry Collector
#
# NOTE: We cannot use the base otel/opentelemetry-collector or otel/opentelemetry-collector-contrib
# Docker images because they don't include custom/third-party exporters like mssql.
# Custom components require building a new collector binary using OCB (OpenTelemetry Collector Builder).
#
FROM golang:1.25 AS builder

# Install the OpenTelemetry Collector Builder
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.145.0

WORKDIR /build

# Copy the builder config and source code
COPY otelcol-builder.yaml .
COPY src/ ./src/

# Build the custom collector
RUN builder --config=otelcol-builder.yaml

# Stage 2: Create the final image
FROM gcr.io/distroless/base-debian12:nonroot

USER nonroot:nonroot

COPY --from=builder --chown=nonroot:nonroot /build/otelcol-mssql/otelcol-mssql /otelcol-mssql

EXPOSE 4317 4318 55679

ENTRYPOINT ["/otelcol-mssql"]
CMD ["--config", "/etc/otelcol/config.yaml"]
