// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package mssqlexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "mssql", factory.Type().String())
}

func TestDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	require.NotNil(t, cfg)
	mssqlCfg, ok := cfg.(*Config)
	require.True(t, ok)

	assert.Equal(t, "", mssqlCfg.ConnectionString)
	assert.Equal(t, 1000, mssqlCfg.BatchSize)
}

func TestCreateTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "sqlserver://user:pass@localhost:1433?database=otel"

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateTraces(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)
}

func TestCreateLogsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "sqlserver://user:pass@localhost:1433?database=otel"

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateLogs(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)
}

func TestCreateMetricsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "sqlserver://user:pass@localhost:1433?database=otel"

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateMetrics(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)
}

func TestCreateTracesExporterInvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	// Connection string is empty, should fail validation
	cfg.ConnectionString = ""

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateTraces(context.Background(), set, cfg)
	require.Error(t, err)
	require.Nil(t, exp)
}

func TestCreateLogsExporterInvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	// Connection string is empty, should fail validation
	cfg.ConnectionString = ""

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateLogs(context.Background(), set, cfg)
	require.Error(t, err)
	require.Nil(t, exp)
}

func TestCreateMetricsExporterInvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	// Connection string is empty, should fail validation
	cfg.ConnectionString = ""

	set := exportertest.NewNopSettings(component.MustNewType("mssql"))

	exp, err := factory.CreateMetrics(context.Background(), set, cfg)
	require.Error(t, err)
	require.Nil(t, exp)
}

func TestFactoryLifecycle(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)

	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)

	err := componenttest.CheckConfigStruct(cfg)
	assert.NoError(t, err)
}
