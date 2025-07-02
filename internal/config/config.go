package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config represents the main configuration structure
type Config struct {
	Migration      MigrationConfig                 `json:"migration"`
	Parallelism    ParallelismConfig               `json:"parallelism"`
	Firestore      FirestoreConfig                 `json:"firestore"`
	DatabasePairs  []DatabasePair                  `json:"databasePairs"`
	PairProcessing map[string]PairProcessingConfig `json:"pairProcessing,omitempty"`
	Checkpoint     CheckpointConfig                `json:"checkpoint"`
	Logging        LoggingConfig                   `json:"logging"`
	Monitoring     MonitoringConfig                `json:"monitoring,omitempty"`
}

// MigrationConfig contains migration-specific settings
type MigrationConfig struct {
	Mode                string `json:"mode"`                // "migrate" or "live"
	BatchSize           int    `json:"batchSize"`           // Number of items per batch
	CheckpointFrequency int    `json:"checkpointFrequency"` // Save checkpoint every N records
	MaxRetries          int    `json:"maxRetries"`          // Maximum retry attempts
	RetryDelayMs        int    `json:"retryDelayMs"`        // Base retry delay in milliseconds
}

// ParallelismConfig contains parallelism settings
type ParallelismConfig struct {
	DynamoDBReaders    int `json:"dynamodbReaders"`    // Number of parallel DynamoDB readers
	FirestoreWriters   int `json:"firestoreWriters"`   // Number of parallel Firestore writers
	SegmentCount       int `json:"segmentCount"`       // DynamoDB parallel scan segments
	MaxConcurrentPairs int `json:"maxConcurrentPairs"` // Max pairs to process simultaneously
}

// FirestoreConfig contains global Firestore connection settings
type FirestoreConfig struct {
	ConnectionString string              `json:"connectionString"`
	WriteConcern     *WriteConcernConfig `json:"writeConcern,omitempty"`
	ReadPreference   string              `json:"readPreference,omitempty"`
	ReadConcern      string              `json:"readConcern,omitempty"`
}

// WriteConcernConfig represents MongoDB write concern settings
type WriteConcernConfig struct {
	W        interface{} `json:"w"`        // Write concern (int or string like "majority")
	J        bool        `json:"j"`        // Journal acknowledgment
	WTimeout int         `json:"wtimeout"` // Write timeout in milliseconds
}

// DatabasePair represents a source-target database pair
type DatabasePair struct {
	Name    string        `json:"name"`
	Source  SourceConfig  `json:"source"`
	Target  TargetConfig  `json:"target"`
	Mapping MappingConfig `json:"mapping"`
}

// SourceConfig contains DynamoDB source configuration
type SourceConfig struct {
	Region     string      `json:"region"`
	TableName  string      `json:"tableName"`
	Endpoint   string      `json:"endpoint,omitempty"` // For DynamoDB Local
	ScanConfig *ScanConfig `json:"scanConfig,omitempty"`
}

// ScanConfig contains DynamoDB scan-specific settings
type ScanConfig struct {
	ConsistentRead            bool                   `json:"consistentRead"`
	ProjectionExpression      string                 `json:"projectionExpression,omitempty"`
	FilterExpression          string                 `json:"filterExpression,omitempty"`
	ExpressionAttributeNames  map[string]string      `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]interface{} `json:"expressionAttributeValues,omitempty"`
}

// TargetConfig contains MongoDB target configuration
type TargetConfig struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

// MappingConfig contains data transformation settings
type MappingConfig struct {
	CollectionNaming  string             `json:"collectionNaming"`            // "preserve", "snake_case", "camelCase"
	FieldNaming       string             `json:"fieldNaming"`                 // "preserve", "snake_case", "camelCase"
	IndexCreation     bool               `json:"indexCreation"`               // Auto-create indexes
	PrimaryKeyMapping *PrimaryKeyMapping `json:"primaryKeyMapping,omitempty"` // Primary key field mapping
	CustomTransforms  []TransformConfig  `json:"customTransforms,omitempty"`
}

// PrimaryKeyMapping defines how to map DynamoDB primary key to MongoDB _id
type PrimaryKeyMapping struct {
	SourceField string `json:"sourceField"` // DynamoDB field name (e.g., "id", "userId")
	TargetField string `json:"targetField"` // MongoDB field name (e.g., "_id")
}

// TransformConfig represents field transformation rules
type TransformConfig struct {
	Field string `json:"field"` // Field path (supports dot notation)
	Type  string `json:"type"`  // Target type: "datetime", "decimal128", "array", "object"
}

// PairProcessingConfig contains per-pair processing overrides
type PairProcessingConfig struct {
	BatchSize       int `json:"batchSize,omitempty"`
	MaxRetries      int `json:"maxRetries,omitempty"`
	ParallelReaders int `json:"parallelReaders,omitempty"`
	ParallelWriters int `json:"parallelWriters,omitempty"`
}

// CheckpointConfig contains checkpoint storage settings
type CheckpointConfig struct {
	Storage          string `json:"storage"`                    // "file" or "mongodb"
	Path             string `json:"path,omitempty"`             // For file storage
	ConnectionString string `json:"connectionString,omitempty"` // For MongoDB storage
	Database         string `json:"database,omitempty"`         // For MongoDB storage
	Collection       string `json:"collection,omitempty"`       // For MongoDB storage
	RetentionDays    int    `json:"retentionDays,omitempty"`    // Checkpoint retention
	Compression      bool   `json:"compression,omitempty"`      // Compress checkpoint files
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level       string `json:"level"`                 // "debug", "info", "warn", "error"
	Format      string `json:"format,omitempty"`      // "json", "text"
	Output      string `json:"output,omitempty"`      // "stdout", "file", "both"
	File        string `json:"file,omitempty"`        // Log file path
	PerPairLogs bool   `json:"perPairLogs,omitempty"` // Separate log files per pair
}

// MonitoringConfig contains monitoring and metrics settings
type MonitoringConfig struct {
	MetricsEnabled  bool `json:"metricsEnabled,omitempty"`
	MetricsPort     int  `json:"metricsPort,omitempty"`
	HealthCheckPort int  `json:"healthCheckPort,omitempty"`
	PairMetrics     bool `json:"pairMetrics,omitempty"`
}

// Default configuration values
var DefaultConfig = Config{
	Migration: MigrationConfig{
		Mode:                "migrate",
		BatchSize:           1000,
		CheckpointFrequency: 100,
		MaxRetries:          3,
		RetryDelayMs:        1000,
	},
	Parallelism: ParallelismConfig{
		DynamoDBReaders:    4,
		FirestoreWriters:   4,
		SegmentCount:       8,
		MaxConcurrentPairs: 1,
	},
	Firestore: FirestoreConfig{
		ReadPreference: "primary",
		WriteConcern: &WriteConcernConfig{
			W:        "majority",
			J:        true,
			WTimeout: 5000,
		},
	},
	Checkpoint: CheckpointConfig{
		Storage:       "file",
		Path:          "./checkpoints",
		RetentionDays: 30,
		Compression:   false,
	},
	Logging: LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	},
	Monitoring: MonitoringConfig{
		MetricsEnabled:  false,
		MetricsPort:     8080,
		HealthCheckPort: 8081,
		PairMetrics:     false,
	},
}

// Load loads configuration from a JSON file
func Load(configPath string) (*Config, error) {
	// Start with default configuration
	cfg := DefaultConfig

	// Read configuration file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Expand environment variables
	expandedData := expandEnvVars(string(data))

	// Parse JSON
	if err := json.Unmarshal([]byte(expandedData), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate migration settings
	if c.Migration.Mode != "migrate" && c.Migration.Mode != "live" {
		return fmt.Errorf("invalid migration mode: %s (must be 'migrate' or 'live')", c.Migration.Mode)
	}

	if c.Migration.BatchSize <= 0 {
		return fmt.Errorf("batchSize must be positive, got: %d", c.Migration.BatchSize)
	}

	if c.Migration.CheckpointFrequency <= 0 {
		return fmt.Errorf("checkpointFrequency must be positive, got: %d", c.Migration.CheckpointFrequency)
	}

	// Validate Firestore connection
	if c.Firestore.ConnectionString == "" {
		return fmt.Errorf("firestore.connectionString is required")
	}

	// Validate database pairs
	if len(c.DatabasePairs) == 0 {
		return fmt.Errorf("at least one database pair is required")
	}

	pairNames := make(map[string]bool)
	for i, pair := range c.DatabasePairs {
		if pair.Name == "" {
			return fmt.Errorf("database pair %d: name is required", i)
		}

		if pairNames[pair.Name] {
			return fmt.Errorf("duplicate database pair name: %s", pair.Name)
		}
		pairNames[pair.Name] = true

		if err := pair.Validate(); err != nil {
			return fmt.Errorf("database pair %s: %w", pair.Name, err)
		}
	}

	// Validate checkpoint configuration
	if c.Checkpoint.Storage != "file" && c.Checkpoint.Storage != "mongodb" {
		return fmt.Errorf("invalid checkpoint storage: %s (must be 'file' or 'mongodb')", c.Checkpoint.Storage)
	}

	if c.Checkpoint.Storage == "file" && c.Checkpoint.Path == "" {
		return fmt.Errorf("checkpoint path is required for file storage")
	}

	if c.Checkpoint.Storage == "mongodb" {
		if c.Checkpoint.ConnectionString == "" {
			return fmt.Errorf("checkpoint connectionString is required for mongodb storage")
		}
		if c.Checkpoint.Database == "" {
			return fmt.Errorf("checkpoint database is required for mongodb storage")
		}
		if c.Checkpoint.Collection == "" {
			return fmt.Errorf("checkpoint collection is required for mongodb storage")
		}
	}

	return nil
}

// Validate validates a database pair configuration
func (dp *DatabasePair) Validate() error {
	// Validate source
	if dp.Source.Region == "" {
		return fmt.Errorf("source.region is required")
	}
	if dp.Source.TableName == "" {
		return fmt.Errorf("source.tableName is required")
	}

	// Validate target
	if dp.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if dp.Target.Collection == "" {
		return fmt.Errorf("target.collection is required")
	}

	// Validate mapping
	validNamingConventions := map[string]bool{
		"preserve":   true,
		"snake_case": true,
		"camelCase":  true,
	}

	if !validNamingConventions[dp.Mapping.CollectionNaming] {
		return fmt.Errorf("invalid collectionNaming: %s", dp.Mapping.CollectionNaming)
	}

	if !validNamingConventions[dp.Mapping.FieldNaming] {
		return fmt.Errorf("invalid fieldNaming: %s", dp.Mapping.FieldNaming)
	}

	// Validate primary key mapping
	if dp.Mapping.PrimaryKeyMapping != nil {
		if dp.Mapping.PrimaryKeyMapping.SourceField == "" {
			return fmt.Errorf("primaryKeyMapping.sourceField is required")
		}
		if dp.Mapping.PrimaryKeyMapping.TargetField == "" {
			return fmt.Errorf("primaryKeyMapping.targetField is required")
		}
	}

	// Validate custom transforms
	validTransformTypes := map[string]bool{
		"datetime":   true,
		"decimal128": true,
		"array":      true,
		"object":     true,
	}

	for _, transform := range dp.Mapping.CustomTransforms {
		if transform.Field == "" {
			return fmt.Errorf("transform field is required")
		}
		if !validTransformTypes[transform.Type] {
			return fmt.Errorf("invalid transform type: %s", transform.Type)
		}
	}

	return nil
}

// expandEnvVars expands environment variables in the format ${VAR_NAME}
func expandEnvVars(input string) string {
	return os.Expand(input, func(key string) string {
		return os.Getenv(key)
	})
}

// GetPairConfig returns the effective configuration for a specific pair
func (c *Config) GetPairConfig(pairName string) PairProcessingConfig {
	// Start with global defaults
	pairConfig := PairProcessingConfig{
		BatchSize:       c.Migration.BatchSize,
		MaxRetries:      c.Migration.MaxRetries,
		ParallelReaders: c.Parallelism.DynamoDBReaders,
		ParallelWriters: c.Parallelism.FirestoreWriters,
	}

	// Override with pair-specific settings if they exist
	if override, exists := c.PairProcessing[pairName]; exists {
		if override.BatchSize > 0 {
			pairConfig.BatchSize = override.BatchSize
		}
		if override.MaxRetries > 0 {
			pairConfig.MaxRetries = override.MaxRetries
		}
		if override.ParallelReaders > 0 {
			pairConfig.ParallelReaders = override.ParallelReaders
		}
		if override.ParallelWriters > 0 {
			pairConfig.ParallelWriters = override.ParallelWriters
		}
	}

	return pairConfig
}

// ApplyNamingConvention applies the specified naming convention to a string
func ApplyNamingConvention(input, convention string) string {
	switch convention {
	case "snake_case":
		return toSnakeCase(input)
	case "camelCase":
		return toCamelCase(input)
	case "preserve":
		fallthrough
	default:
		return input
	}
}

// toSnakeCase converts a string to snake_case
func toSnakeCase(input string) string {
	var result strings.Builder
	for i, r := range input {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// toCamelCase converts a string to camelCase
func toCamelCase(input string) string {
	parts := strings.Split(input, "_")
	if len(parts) == 1 {
		return input
	}

	var result strings.Builder
	result.WriteString(parts[0])
	for _, part := range parts[1:] {
		if len(part) > 0 {
			result.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	return result.String()
}
