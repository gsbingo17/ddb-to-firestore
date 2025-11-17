package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ddb-to-firestore/internal/checkpoint"
	"ddb-to-firestore/internal/config"
	"ddb-to-firestore/internal/converter"
	"ddb-to-firestore/internal/dynamodb"
	"ddb-to-firestore/internal/firestore"
	"ddb-to-firestore/internal/utils"

	"go.uber.org/zap"
)

// Migrator handles one-time migration from DynamoDB to Firestore
type Migrator struct {
	config          *config.Config
	pair            config.DatabasePair
	dynamoClient    *dynamodb.Client
	firestoreClient *firestore.Client
	checkpointMgr   *checkpoint.Manager
	logger          *zap.Logger
	converter       *converter.DocumentConverter
	writerPool      *firestore.WriterPool // New field for the writer pool
}

// Migrate performs the migration for a single database pair
func (m *Migrator) Migrate(ctx context.Context, resume bool) error {
	m.logger.Info("Starting migration",
		zap.String("source", m.pair.Source.TableName),
		zap.String("target", fmt.Sprintf("%s.%s", m.pair.Target.Database, m.pair.Target.Collection)),
		zap.Bool("resume", resume))

	// Initialize converter
	m.converter = converter.NewDocumentConverter(m.pair.Mapping, m.logger)

	// Get or create checkpoint
	var migrationCheckpoint *checkpoint.Checkpoint
	var err error

	if resume {
		migrationCheckpoint, err = m.checkpointMgr.Load(m.pair.Source.TableName)
		if err != nil {
			return fmt.Errorf("failed to load checkpoint: %w", err)
		}
	}

	if migrationCheckpoint == nil {
		// Create new checkpoint
		segmentCount := m.config.Parallelism.SegmentCount
		migrationCheckpoint = m.checkpointMgr.CreateMigrateCheckpoint(m.pair.Source.TableName, segmentCount)

		if err := m.checkpointMgr.Save(m.pair.Source.TableName, migrationCheckpoint); err != nil {
			return fmt.Errorf("failed to save initial checkpoint: %w", err)
		}
	}

	// Create indexes if configured
	if m.pair.Mapping.IndexCreation {
		if err := m.createIndexes(ctx); err != nil {
			m.logger.Warn("Failed to create indexes", zap.Error(err))
		}
	}

	// Perform parallel migration
	return m.migrateParallel(ctx, migrationCheckpoint)
}

// migrateParallel performs parallel migration using multiple segments
func (m *Migrator) migrateParallel(ctx context.Context, migrationCheckpoint *checkpoint.Checkpoint) error {
	segmentCount := migrationCheckpoint.TotalSegments
	if segmentCount <= 0 {
		segmentCount = 1
	}

	// Create semaphore for controlling parallelism
	pairConfig := m.config.GetPairConfig(m.pair.Name)
	semaphore := make(chan struct{}, pairConfig.ParallelReaders)

	var wg sync.WaitGroup
	errChan := make(chan error, segmentCount)

	m.logger.Info("Starting parallel migration",
		zap.Int("segments", segmentCount),
		zap.Int("parallel_readers", pairConfig.ParallelReaders))

	// Process each segment
	for segment := 0; segment < segmentCount; segment++ {
		// Skip completed segments
		if m.checkpointMgr.IsSegmentCompleted(migrationCheckpoint, segment) {
			m.logger.Info("Skipping completed segment", zap.Int("segment", segment))
			continue
		}

		wg.Add(1)
		go func(seg int) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := m.migrateSegment(ctx, migrationCheckpoint, seg, segmentCount); err != nil {
				errChan <- fmt.Errorf("segment %d failed: %w", seg, err)
			}
		}(segment)
	}

	// Wait for all segments to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("migration failed with %d errors: %v", len(errors), errors)
	}

	m.logger.Info("Migration completed successfully",
		zap.String("table", m.pair.Source.TableName),
		zap.Int64("total_processed", migrationCheckpoint.ProcessedCount))

	return nil
}

// migrateSegment migrates a single segment
func (m *Migrator) migrateSegment(ctx context.Context, migrationCheckpoint *checkpoint.Checkpoint, segment, totalSegments int) error {
	segmentLogger := m.logger.With(zap.Int("segment", segment))
	segmentLogger.Info("Starting segment migration")

	pairConfig := m.config.GetPairConfig(m.pair.Name)
	var processedCount int64
	var lastEvaluatedKey map[string]interface{}

	// Resume from checkpoint if available
	lastEvaluatedKey = m.checkpointMgr.GetSegmentLastKey(migrationCheckpoint, segment)

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Scan DynamoDB
		result, err := m.dynamoClient.Scan(ctx, segment, totalSegments, lastEvaluatedKey)
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		if len(result.Items) == 0 {
			break
		}

		// Convert and write to MongoDB
		if err := m.processBatch(ctx, result.Items, segmentLogger); err != nil {
			return fmt.Errorf("batch processing failed: %w", err)
		}

		processedCount += int64(len(result.Items))
		lastEvaluatedKey = result.LastEvaluatedKey

		// Update checkpoint periodically
		if processedCount%int64(pairConfig.BatchSize) == 0 {
			m.checkpointMgr.UpdateSegmentProgress(
				migrationCheckpoint,
				segment,
				lastEvaluatedKey,
				processedCount,
				false,
			)

			if err := m.checkpointMgr.Save(m.pair.Source.TableName, migrationCheckpoint); err != nil {
				segmentLogger.Warn("Failed to save checkpoint", zap.Error(err))
			}

			// Log progress
			elapsed := time.Since(startTime)
			rate := float64(processedCount) / elapsed.Seconds()
			utils.LogProgress(segmentLogger, m.pair.Source.TableName, processedCount, 0, rate)
		}

		// Check if we're done with this segment
		if result.LastEvaluatedKey == nil {
			break
		}
	}

	// Mark segment as completed
	m.checkpointMgr.UpdateSegmentProgress(
		migrationCheckpoint,
		segment,
		lastEvaluatedKey,
		processedCount,
		true,
	)

	if err := m.checkpointMgr.Save(m.pair.Source.TableName, migrationCheckpoint); err != nil {
		segmentLogger.Warn("Failed to save final checkpoint", zap.Error(err))
	}

	elapsed := time.Since(startTime)
	segmentLogger.Info("Segment migration completed",
		zap.Int64("processed", processedCount),
		zap.Duration("elapsed", elapsed))

	return nil
}

// processBatch processes a batch of DynamoDB items
func (m *Migrator) processBatch(ctx context.Context, items []map[string]interface{}, logger *zap.Logger) error {
	if len(items) == 0 {
		return nil
	}

	pairConfig := m.config.GetPairConfig(m.pair.Name)
	batchSize := pairConfig.BatchSize

	// Process items in smaller batches for MongoDB
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := m.processBatchWithRetry(ctx, batch, logger); err != nil {
			return fmt.Errorf("batch processing failed: %w", err)
		}

		utils.LogBatchProgress(logger, m.pair.Source.TableName, len(batch), i/batchSize+1, "")
	}

	return nil
}

// processBatchWithRetry processes a batch with retry logic
func (m *Migrator) processBatchWithRetry(ctx context.Context, items []map[string]interface{}, logger *zap.Logger) error {
	var operations []firestore.Operation

	// Convert DynamoDB items to Firestore operations
	for _, item := range items {
		// Convert item using the converter
		documentID, firestoreDoc, err := m.converter.ConvertDocument(item)
		if err != nil {
			logger.Warn("Failed to convert document", zap.Error(err), zap.Any("item", item))
			continue
		}

		// Create upsert operation
		operation := &firestore.UpsertOperation{
			DocumentID:  documentID,
			Document:    firestoreDoc,
			OriginalKey: item, // Store original DynamoDB item as key
		}
		operations = append(operations, operation)
	}

	if len(operations) == 0 {
		return nil
	}

	// Execute with retry using the writer pool
	// The writer pool handles its own retries internally, so we just submit the job once.
	// The context passed to Submit will be used by the worker.
	job := firestore.WriteJob{
		Database:   m.pair.Target.Database,
		Collection: m.pair.Target.Collection,
		Operations: operations,
		ResultChan: make(chan error, 1), // Buffered channel for the result
	}

	m.writerPool.Submit(job)

	// Wait for the job to complete and get the result
	select {
	case err := <-job.ResultChan:
		if err != nil {
			return fmt.Errorf("writer pool job failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err() // Context cancelled while waiting for job
	}
}

// createIndexes creates indexes on the MongoDB collection
func (m *Migrator) createIndexes(ctx context.Context) error {
	// TODO: Implement index creation based on DynamoDB key schema
	// For now, just ensure the default _id index exists
	m.logger.Info("Index creation not yet implemented")
	return nil
}

// MigrateSimple performs simple migration without checkpointing (for live mode)
func (m *Migrator) MigrateSimple(ctx context.Context) error {
	m.logger.Info("Starting simple migration (no checkpointing)",
		zap.String("source", m.pair.Source.TableName),
		zap.String("target", fmt.Sprintf("%s.%s", m.pair.Target.Database, m.pair.Target.Collection)))

	// Initialize converter
	m.converter = converter.NewDocumentConverter(m.pair.Mapping, m.logger)

	// Create indexes if configured
	if m.pair.Mapping.IndexCreation {
		if err := m.createIndexes(ctx); err != nil {
			m.logger.Warn("Failed to create indexes", zap.Error(err))
		}
	}

	// Perform simple migration without checkpointing
	return m.migrateSimpleParallel(ctx)
}

// migrateSimpleParallel performs parallel migration without checkpointing
func (m *Migrator) migrateSimpleParallel(ctx context.Context) error {
	segmentCount := m.config.Parallelism.SegmentCount
	if segmentCount <= 0 {
		segmentCount = 1
	}

	// Create semaphore for controlling parallelism
	pairConfig := m.config.GetPairConfig(m.pair.Name)
	semaphore := make(chan struct{}, pairConfig.ParallelReaders)

	var wg sync.WaitGroup
	errChan := make(chan error, segmentCount)

	m.logger.Info("Starting simple parallel migration",
		zap.Int("segments", segmentCount),
		zap.Int("parallel_readers", pairConfig.ParallelReaders),
		zap.String("mode", "no_checkpointing"))

	// Process each segment without checkpointing
	for segment := 0; segment < segmentCount; segment++ {
		wg.Add(1)
		go func(seg int) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := m.migrateSegmentSimple(ctx, seg, segmentCount); err != nil {
				errChan <- fmt.Errorf("segment %d failed: %w", seg, err)
			}
		}(segment)
	}

	// Wait for all segments to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("simple migration failed with %d errors: %v", len(errors), errors)
	}

	m.logger.Info("Simple migration completed successfully",
		zap.String("table", m.pair.Source.TableName))

	return nil
}

// migrateSegmentSimple migrates a single segment without checkpointing
func (m *Migrator) migrateSegmentSimple(ctx context.Context, segment, totalSegments int) error {
	segmentLogger := m.logger.With(zap.Int("segment", segment))
	segmentLogger.Info("Starting simple segment migration (no checkpointing)")

	var processedCount int64
	var lastEvaluatedKey map[string]interface{}

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Scan DynamoDB
		result, err := m.dynamoClient.Scan(ctx, segment, totalSegments, lastEvaluatedKey)
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		if len(result.Items) == 0 {
			break
		}

		// Convert and write to Firestore
		if err := m.processBatch(ctx, result.Items, segmentLogger); err != nil {
			return fmt.Errorf("batch processing failed: %w", err)
		}

		processedCount += int64(len(result.Items))
		lastEvaluatedKey = result.LastEvaluatedKey

		// Log progress periodically (no checkpoint saving)
		pairConfig := m.config.GetPairConfig(m.pair.Name)
		if processedCount%int64(pairConfig.BatchSize*10) == 0 {
			elapsed := time.Since(startTime)
			rate := float64(processedCount) / elapsed.Seconds()
			segmentLogger.Info("Simple migration progress",
				zap.Int64("processed", processedCount),
				zap.Float64("rate_per_sec", rate),
				zap.Duration("elapsed", elapsed))
		}

		// Check if we're done with this segment
		if result.LastEvaluatedKey == nil {
			break
		}
	}

	elapsed := time.Since(startTime)
	segmentLogger.Info("Simple segment migration completed",
		zap.Int64("processed", processedCount),
		zap.Duration("elapsed", elapsed))

	return nil
}
