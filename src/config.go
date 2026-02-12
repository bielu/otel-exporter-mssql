// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: MIT

package mssqlexporter

import (
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/config/configretry"
)

// Config defines configuration for the MSSQL exporter.
type Config struct {
	// ConnectionString is the MSSQL connection string.
	// Format: sqlserver://username:password@host:port
	// If Database is specified separately, it will be appended to the connection string.
	ConnectionString string `mapstructure:"connection_string"`

	// Database is the name of the database to use.
	// If specified, it will be appended to the connection string.
	// This allows specifying the database separately from the connection string.
	Database string `mapstructure:"database"`

	// TablePrefix is an optional prefix for all table names.
	// For example, if set to "otel_", tables will be named "otel_Resources", "otel_Spans", etc.
	TablePrefix string `mapstructure:"table_prefix"`

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

// GetConnectionString returns the full connection string including the database if specified.
func (cfg *Config) GetConnectionString() string {
	connStr := cfg.ConnectionString
	if cfg.Database != "" {
		// Check if connection string already has database parameter
		if !strings.Contains(strings.ToLower(connStr), "database=") {
			if strings.Contains(connStr, "?") {
				connStr = fmt.Sprintf("%s&database=%s", connStr, cfg.Database)
			} else {
				connStr = fmt.Sprintf("%s?database=%s", connStr, cfg.Database)
			}
		}
	}
	return connStr
}

// TableName returns the table name with the configured prefix.
func (cfg *Config) TableName(name string) string {
	return cfg.TablePrefix + name
}
