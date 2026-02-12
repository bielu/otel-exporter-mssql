// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package mssqlexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func TestNewMSSQLExporter(t *testing.T) {
	cfg := &Config{
		ConnectionString: "sqlserver://user:pass@localhost:1433?database=otel",
		BatchSize:        1000,
		RetrySettings:    configretry.NewDefaultBackOffConfig(),
	}

	exp, err := newMSSQLExporter(cfg, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, exp)
	assert.Equal(t, cfg, exp.cfg)
}

func TestNewMSSQLExporterInvalidConfig(t *testing.T) {
	cfg := &Config{
		ConnectionString: "",
		BatchSize:        1000,
		RetrySettings:    configretry.NewDefaultBackOffConfig(),
	}

	exp, err := newMSSQLExporter(cfg, zap.NewNop())
	require.Error(t, err)
	require.Nil(t, exp)
}

func TestExtractAttributeValues(t *testing.T) {
	tests := []struct {
		name       string
		value      pcommon.Value
		wantString interface{}
		wantInt    interface{}
		wantDouble interface{}
		wantBool   interface{}
	}{
		{
			name:       "string value",
			value:      func() pcommon.Value { v := pcommon.NewValueStr("test"); return v }(),
			wantString: "test",
			wantInt:    nil,
			wantDouble: nil,
			wantBool:   nil,
		},
		{
			name:       "int value",
			value:      func() pcommon.Value { v := pcommon.NewValueInt(42); return v }(),
			wantString: nil,
			wantInt:    int64(42),
			wantDouble: nil,
			wantBool:   nil,
		},
		{
			name:       "double value",
			value:      func() pcommon.Value { v := pcommon.NewValueDouble(3.14); return v }(),
			wantString: nil,
			wantInt:    nil,
			wantDouble: 3.14,
			wantBool:   nil,
		},
		{
			name:       "bool value true",
			value:      func() pcommon.Value { v := pcommon.NewValueBool(true); return v }(),
			wantString: nil,
			wantInt:    nil,
			wantDouble: nil,
			wantBool:   true,
		},
		{
			name:       "bool value false",
			value:      func() pcommon.Value { v := pcommon.NewValueBool(false); return v }(),
			wantString: nil,
			wantInt:    nil,
			wantDouble: nil,
			wantBool:   false,
		},
		{
			name:       "empty value",
			value:      func() pcommon.Value { v := pcommon.NewValueEmpty(); return v }(),
			wantString: "",
			wantInt:    nil,
			wantDouble: nil,
			wantBool:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stringVal, intVal, doubleVal, boolVal := extractAttributeValues(tt.value)
			assert.Equal(t, tt.wantString, stringVal)
			assert.Equal(t, tt.wantInt, intVal)
			assert.Equal(t, tt.wantDouble, doubleVal)
			assert.Equal(t, tt.wantBool, boolVal)
		})
	}
}

func TestGetMetricType(t *testing.T) {
	tests := []struct {
		name     string
		metric   pmetric.Metric
		expected int
	}{
		{
			name: "gauge",
			metric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				m.SetEmptyGauge()
				return m
			}(),
			expected: 1,
		},
		{
			name: "sum",
			metric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				m.SetEmptySum()
				return m
			}(),
			expected: 2,
		},
		{
			name: "histogram",
			metric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				m.SetEmptyHistogram()
				return m
			}(),
			expected: 3,
		},
		{
			name: "exponential histogram",
			metric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				m.SetEmptyExponentialHistogram()
				return m
			}(),
			expected: 4,
		},
		{
			name: "summary",
			metric: func() pmetric.Metric {
				m := pmetric.NewMetric()
				m.SetEmptySummary()
				return m
			}(),
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getMetricType(tt.metric)
			assert.Equal(t, tt.expected, result)
		})
	}
}
