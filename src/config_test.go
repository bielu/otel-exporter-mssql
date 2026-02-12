// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: MIT

package mssqlexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configretry"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?database=otel",
				BatchSize:        1000,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: false,
		},
		{
			name: "valid config with separate database",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433",
				Database:         "otel",
				BatchSize:        1000,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: false,
		},
		{
			name: "valid config with table prefix",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433",
				Database:         "otel",
				TablePrefix:      "otel_",
				BatchSize:        1000,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: false,
		},
		{
			name: "missing connection string",
			config: Config{
				ConnectionString: "",
				BatchSize:        1000,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: true,
			errMsg:  "connection_string is required",
		},
		{
			name: "invalid batch size - zero",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?database=otel",
				BatchSize:        0,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: true,
			errMsg:  "batch_size must be greater than 0",
		},
		{
			name: "invalid batch size - negative",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?database=otel",
				BatchSize:        -1,
				RetrySettings:    configretry.NewDefaultBackOffConfig(),
			},
			wantErr: true,
			errMsg:  "batch_size must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "connection string with database in URL",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?database=otel",
			},
			expected: "sqlserver://user:pass@localhost:1433?database=otel",
		},
		{
			name: "separate database with no query params",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433",
				Database:         "otel",
			},
			expected: "sqlserver://user:pass@localhost:1433?database=otel",
		},
		{
			name: "separate database with existing query params",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?encrypt=true",
				Database:         "otel",
			},
			expected: "sqlserver://user:pass@localhost:1433?encrypt=true&database=otel",
		},
		{
			name: "no separate database when already in connection string",
			config: Config{
				ConnectionString: "sqlserver://user:pass@localhost:1433?database=existing",
				Database:         "otel",
			},
			expected: "sqlserver://user:pass@localhost:1433?database=existing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.GetConnectionString()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTableName(t *testing.T) {
	tests := []struct {
		name        string
		tablePrefix string
		tableName   string
		expected    string
	}{
		{
			name:        "no prefix",
			tablePrefix: "",
			tableName:   "Resources",
			expected:    "Resources",
		},
		{
			name:        "with prefix",
			tablePrefix: "otel_",
			tableName:   "Resources",
			expected:    "otel_Resources",
		},
		{
			name:        "custom prefix",
			tablePrefix: "telemetry_",
			tableName:   "Spans",
			expected:    "telemetry_Spans",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{TablePrefix: tt.tablePrefix}
			result := cfg.TableName(tt.tableName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
