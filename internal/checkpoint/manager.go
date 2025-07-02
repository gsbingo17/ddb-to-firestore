package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"ddb-to-firestore/internal/config"

	"go.uber.org/zap"
)

// Checkpoint represents a migration checkpoint
type Checkpoint struct {
	TableName                string                 `json:"tableName"`
	Mode                     string                 `json:"mode"` // "migrate" or "live"
	LastEvaluatedKey         map[string]interface{} `json:"lastEvaluatedKey,omitempty"`
	Timestamp                *time.Time             `json:"timestamp,omitempty"`
	ProcessedCount           int64                  `json:"processedCount"`
	TotalSegments            int                    `json:"totalSegments,omitempty"`
	SegmentProgress          map[int]SegmentInfo    `json:"segmentProgress,omitempty"`
	StreamArn                string                 `json:"streamArn,omitempty"`
	InitialMigrationComplete bool                   `json:"initialMigrationComplete,omitempty"`
	StartTime                time.Time              `json:"startTime"`
	LastUpdated              time.Time              `json:"lastUpdated"`
}

// SegmentInfo represents progress for a DynamoDB scan segment
type SegmentInfo struct {
	LastEvaluatedKey map[string]interface{} `json:"lastEvaluatedKey,omitempty"`
	ProcessedCount   int64                  `json:"processedCount"`
	Completed        bool                   `json:"completed"`
}

// Manager manages checkpoint storage and retrieval
type Manager struct {
	config  config.CheckpointConfig
	logger  *zap.Logger
	storage Storage
	mu      sync.RWMutex
}

// Storage interface for checkpoint persistence
type Storage interface {
	Save(tableName string, checkpoint *Checkpoint) error
	Load(tableName string) (*Checkpoint, error)
	Delete(tableName string) error
	List() ([]string, error)
	Close() error
}

// NewManager creates a new checkpoint manager
func NewManager(cfg config.CheckpointConfig, logger *zap.Logger) (*Manager, error) {
	var storage Storage
	var err error

	switch cfg.Storage {
	case "file":
		storage, err = NewFileStorage(cfg, logger)
	case "mongodb":
		storage, err = NewMongoStorage(cfg, logger)
	default:
		return nil, fmt.Errorf("unsupported checkpoint storage: %s", cfg.Storage)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create checkpoint storage: %w", err)
	}

	return &Manager{
		config:  cfg,
		logger:  logger,
		storage: storage,
	}, nil
}

// Close closes the checkpoint manager
func (m *Manager) Close() error {
	return m.storage.Close()
}

// Save saves a checkpoint
func (m *Manager) Save(tableName string, checkpoint *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	checkpoint.LastUpdated = time.Now()

	if err := m.storage.Save(tableName, checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint for %s: %w", tableName, err)
	}

	m.logger.Debug("Checkpoint saved",
		zap.String("table", tableName),
		zap.Time("timestamp", checkpoint.LastUpdated),
		zap.Int64("processed", checkpoint.ProcessedCount))

	return nil
}

// Load loads a checkpoint
func (m *Manager) Load(tableName string) (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	checkpoint, err := m.storage.Load(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint for %s: %w", tableName, err)
	}

	if checkpoint != nil {
		m.logger.Debug("Checkpoint loaded",
			zap.String("table", tableName),
			zap.Time("timestamp", checkpoint.LastUpdated),
			zap.Int64("processed", checkpoint.ProcessedCount))
	}

	return checkpoint, nil
}

// Delete deletes a checkpoint
func (m *Manager) Delete(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.storage.Delete(tableName); err != nil {
		return fmt.Errorf("failed to delete checkpoint for %s: %w", tableName, err)
	}

	m.logger.Info("Checkpoint deleted", zap.String("table", tableName))
	return nil
}

// List lists all checkpoints
func (m *Manager) List() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.storage.List()
}

// CreateMigrateCheckpoint creates a new checkpoint for migration mode
func (m *Manager) CreateMigrateCheckpoint(tableName string, totalSegments int) *Checkpoint {
	now := time.Now()
	checkpoint := &Checkpoint{
		TableName:       tableName,
		Mode:            "migrate",
		ProcessedCount:  0,
		TotalSegments:   totalSegments,
		SegmentProgress: make(map[int]SegmentInfo),
		StartTime:       now,
		LastUpdated:     now,
	}

	// Initialize segment progress
	for i := 0; i < totalSegments; i++ {
		checkpoint.SegmentProgress[i] = SegmentInfo{
			ProcessedCount: 0,
			Completed:      false,
		}
	}

	return checkpoint
}

// CreateLiveCheckpoint creates a new checkpoint for live replication mode
func (m *Manager) CreateLiveCheckpoint(tableName, streamArn string, startTimestamp *time.Time) *Checkpoint {
	now := time.Now()
	checkpoint := &Checkpoint{
		TableName:                tableName,
		Mode:                     "live",
		Timestamp:                startTimestamp,
		StreamArn:                streamArn,
		InitialMigrationComplete: false, // Start with migration incomplete
		StartTime:                now,
		LastUpdated:              now,
	}

	return checkpoint
}

// UpdateSegmentProgress updates progress for a specific segment
func (m *Manager) UpdateSegmentProgress(checkpoint *Checkpoint, segment int, lastKey map[string]interface{}, processed int64, completed bool) {
	if checkpoint.SegmentProgress == nil {
		checkpoint.SegmentProgress = make(map[int]SegmentInfo)
	}

	checkpoint.SegmentProgress[segment] = SegmentInfo{
		LastEvaluatedKey: lastKey,
		ProcessedCount:   processed,
		Completed:        completed,
	}

	// Update total processed count
	var total int64
	for _, info := range checkpoint.SegmentProgress {
		total += info.ProcessedCount
	}
	checkpoint.ProcessedCount = total
}

// UpdateTimestamp updates the checkpoint timestamp for live replication
func (m *Manager) UpdateTimestamp(checkpoint *Checkpoint, timestamp *time.Time) {
	if timestamp != nil && (checkpoint.Timestamp == nil || timestamp.After(*checkpoint.Timestamp)) {
		checkpoint.Timestamp = timestamp
	}
}

// IsSegmentCompleted checks if a segment is completed
func (m *Manager) IsSegmentCompleted(checkpoint *Checkpoint, segment int) bool {
	if checkpoint.SegmentProgress == nil {
		return false
	}

	info, exists := checkpoint.SegmentProgress[segment]
	return exists && info.Completed
}

// GetSegmentLastKey gets the last evaluated key for a segment
func (m *Manager) GetSegmentLastKey(checkpoint *Checkpoint, segment int) map[string]interface{} {
	if checkpoint.SegmentProgress == nil {
		return nil
	}

	info, exists := checkpoint.SegmentProgress[segment]
	if !exists {
		return nil
	}

	return info.LastEvaluatedKey
}

// GetTimestamp gets the checkpoint timestamp for live replication
func (m *Manager) GetTimestamp(checkpoint *Checkpoint) *time.Time {
	return checkpoint.Timestamp
}

// FileStorage implements file-based checkpoint storage
type FileStorage struct {
	basePath string
	logger   *zap.Logger
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(cfg config.CheckpointConfig, logger *zap.Logger) (*FileStorage, error) {
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	return &FileStorage{
		basePath: cfg.Path,
		logger:   logger,
	}, nil
}

// Save saves a checkpoint to file
func (fs *FileStorage) Save(tableName string, checkpoint *Checkpoint) error {
	filePath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", tableName))
	tempPath := filePath + ".tmp"

	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temporary file first
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	return nil
}

// Load loads a checkpoint from file
func (fs *FileStorage) Load(tableName string) (*Checkpoint, error) {
	filePath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", tableName))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No checkpoint exists
		}
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &checkpoint, nil
}

// Delete deletes a checkpoint file
func (fs *FileStorage) Delete(tableName string) error {
	filePath := filepath.Join(fs.basePath, fmt.Sprintf("%s.json", tableName))

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete checkpoint file: %w", err)
	}

	return nil
}

// List lists all checkpoint files
func (fs *FileStorage) List() ([]string, error) {
	entries, err := os.ReadDir(fs.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint directory: %w", err)
	}

	var tables []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			tableName := entry.Name()[:len(entry.Name())-5] // Remove .json extension
			tables = append(tables, tableName)
		}
	}

	return tables, nil
}

// Close closes the file storage (no-op)
func (fs *FileStorage) Close() error {
	return nil
}

// MongoStorage implements MongoDB-based checkpoint storage
type MongoStorage struct {
	// TODO: Implement MongoDB storage
	logger *zap.Logger
}

// NewMongoStorage creates a new MongoDB-based storage
func NewMongoStorage(cfg config.CheckpointConfig, logger *zap.Logger) (*MongoStorage, error) {
	// TODO: Implement MongoDB checkpoint storage
	return &MongoStorage{
		logger: logger,
	}, nil
}

// Save saves a checkpoint to MongoDB
func (ms *MongoStorage) Save(tableName string, checkpoint *Checkpoint) error {
	// TODO: Implement MongoDB save
	return fmt.Errorf("MongoDB checkpoint storage not implemented yet")
}

// Load loads a checkpoint from MongoDB
func (ms *MongoStorage) Load(tableName string) (*Checkpoint, error) {
	// TODO: Implement MongoDB load
	return nil, fmt.Errorf("MongoDB checkpoint storage not implemented yet")
}

// Delete deletes a checkpoint from MongoDB
func (ms *MongoStorage) Delete(tableName string) error {
	// TODO: Implement MongoDB delete
	return fmt.Errorf("MongoDB checkpoint storage not implemented yet")
}

// List lists all checkpoints from MongoDB
func (ms *MongoStorage) List() ([]string, error) {
	// TODO: Implement MongoDB list
	return nil, fmt.Errorf("MongoDB checkpoint storage not implemented yet")
}

// Close closes the MongoDB storage
func (ms *MongoStorage) Close() error {
	// TODO: Implement MongoDB close
	return nil
}
