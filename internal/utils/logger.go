package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ddb-to-firestore/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger creates a new logger based on the configuration
func NewLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	// Set default values if not specified
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	if cfg.Output == "" {
		cfg.Output = "stdout"
	}

	// Parse log level
	level, err := zap.ParseAtomicLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", cfg.Level, err)
	}

	// Create encoder config
	var encoderConfig zapcore.EncoderConfig
	if cfg.Format == "json" {
		encoderConfig = zap.NewProductionEncoderConfig()
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
	}

	// Customize encoder config
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.LevelKey = "level"
	encoderConfig.MessageKey = "message"
	encoderConfig.CallerKey = "caller"
	encoderConfig.StacktraceKey = "stacktrace"

	// Create encoder
	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Create output writers
	var cores []zapcore.Core

	switch cfg.Output {
	case "stdout":
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level))
	case "file":
		if cfg.File == "" {
			return nil, fmt.Errorf("log file path is required when output is 'file'")
		}
		if err := ensureLogDir(cfg.File); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}
		file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", cfg.File, err)
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(file), level))
	case "both":
		// Add stdout
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level))
		// Add file
		if cfg.File == "" {
			return nil, fmt.Errorf("log file path is required when output is 'both'")
		}
		if err := ensureLogDir(cfg.File); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}
		file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", cfg.File, err)
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(file), level))
	default:
		return nil, fmt.Errorf("invalid log output: %s (must be 'stdout', 'file', or 'both')", cfg.Output)
	}

	// Create logger
	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))

	return logger, nil
}

// NewPairLogger creates a logger for a specific database pair
func NewPairLogger(cfg config.LoggingConfig, pairName string) (*zap.Logger, error) {
	if !cfg.PerPairLogs {
		return NewLogger(cfg)
	}

	// Create pair-specific config
	pairCfg := cfg
	if cfg.File != "" {
		// Insert pair name into file path
		dir := filepath.Dir(cfg.File)
		ext := filepath.Ext(cfg.File)
		base := strings.TrimSuffix(filepath.Base(cfg.File), ext)
		pairCfg.File = filepath.Join(dir, fmt.Sprintf("%s_%s%s", base, pairName, ext))
	}

	logger, err := NewLogger(pairCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pair logger for %s: %w", pairName, err)
	}

	return logger.With(zap.String("pair", pairName)), nil
}

// ensureLogDir creates the log directory if it doesn't exist
func ensureLogDir(logFile string) error {
	dir := filepath.Dir(logFile)
	if dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}

// LogProgress logs migration progress with structured data
func LogProgress(logger *zap.Logger, tableName string, processed, total int64, rate float64) {
	percentage := float64(processed) / float64(total) * 100
	logger.Info("Migration progress",
		zap.String("table", tableName),
		zap.Int64("processed", processed),
		zap.Int64("total", total),
		zap.Float64("percentage", percentage),
		zap.Float64("rate_per_second", rate),
	)
}

// LogBatchProgress logs batch processing progress
func LogBatchProgress(logger *zap.Logger, tableName string, batchSize, batchNum int, duration string) {
	logger.Info("Batch processed",
		zap.String("table", tableName),
		zap.Int("batch_size", batchSize),
		zap.Int("batch_number", batchNum),
		zap.String("duration", duration),
	)
}

// LogError logs an error with context
func LogError(logger *zap.Logger, operation, tableName string, err error, context map[string]interface{}) {
	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("table", tableName),
		zap.Error(err),
	}

	for key, value := range context {
		fields = append(fields, zap.Any(key, value))
	}

	logger.Error("Operation failed", fields...)
}

// LogRetry logs retry attempts
func LogRetry(logger *zap.Logger, operation string, attempt, maxAttempts int, err error, delay string) {
	logger.Warn("Retrying operation",
		zap.String("operation", operation),
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", maxAttempts),
		zap.Error(err),
		zap.String("retry_delay", delay),
	)
}

// LogCheckpoint logs checkpoint operations
func LogCheckpoint(logger *zap.Logger, tableName string, checkpoint interface{}, operation string) {
	logger.Info("Checkpoint operation",
		zap.String("table", tableName),
		zap.String("operation", operation),
		zap.Any("checkpoint", checkpoint),
	)
}

// LogStreamRecord logs DynamoDB stream record processing
func LogStreamRecord(logger *zap.Logger, tableName, eventName, shardID string, recordCount int) {
	logger.Debug("Processing stream records",
		zap.String("table", tableName),
		zap.String("event_name", eventName),
		zap.String("shard_id", shardID),
		zap.Int("record_count", recordCount),
	)
}

// LogConnectionStatus logs database connection status
func LogConnectionStatus(logger *zap.Logger, service, status string, details map[string]interface{}) {
	fields := []zap.Field{
		zap.String("service", service),
		zap.String("status", status),
	}

	for key, value := range details {
		fields = append(fields, zap.Any(key, value))
	}

	logger.Info("Connection status", fields...)
}
