// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package mssqlexporter

import (
	"errors"

	"go.opentelemetry.io/collector/config/configretry"
)

// Config defines configuration for the MSSQL exporter.
type Config struct {
	// ConnectionString is the MSSQL connection string.
	// Format: sqlserver://username:password@host:port?database=dbname
	ConnectionString string `mapstructure:"connection_string"`

	// BatchSize is the maximum number of records to insert in a single batch.
	// Default: 1000
	BatchSize int `mapstructure:"batch_size"`

	// RetrySettings defines retry configuration.
	RetrySettings configretry.BackOffConfig `mapstructure:"retry_on_failure"`
}

// Validate validates the MSSQL exporter configuration.
func (cfg *Config) Validate() error {
	if cfg.ConnectionString == "" {
		return errors.New("connection_string is required")
	}
	if cfg.BatchSize <= 0 {
		return errors.New("batch_size must be greater than 0")
	}
	return nil
}
