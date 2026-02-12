// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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
