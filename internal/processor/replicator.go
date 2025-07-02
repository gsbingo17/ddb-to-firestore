package processor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"ddb-to-firestore/internal/checkpoint"
	"ddb-to-firestore/internal/config"
	"ddb-to-firestore/internal/converter"
	"ddb-to-firestore/internal/dynamodb"
	"ddb-to-firestore/internal/firestore"

	"go.uber.org/zap"
)

// Replicator handles live replication from DynamoDB to Firestore using streams
type Replicator struct {
	config          *config.Config
	pair            config.DatabasePair
	dynamoClient    *dynamodb.Client
	firestoreClient *firestore.Client
	checkpointMgr   *checkpoint.Manager
	logger          *zap.Logger
	converter       *converter.DocumentConverter
	writerPool      *firestore.WriterPool // New field for the writer pool
}

// NewReplicator creates a new Replicator instance
func NewReplicator(config *config.Config, pair config.DatabasePair, dynamoClient *dynamodb.Client, firestoreClient *firestore.Client, checkpointMgr *checkpoint.Manager, logger *zap.Logger, writerPool *firestore.WriterPool) *Replicator {
	logger.Info("Creating new replicator",
		zap.String("pairName", pair.Name),
		zap.Bool("configNil", config == nil),
		zap.Bool("dynamoClientNil", dynamoClient == nil),
		zap.Bool("firestoreClientNil", firestoreClient == nil),
		zap.Bool("checkpointMgrNil", checkpointMgr == nil),
		zap.Bool("loggerNil", logger == nil),
		zap.Bool("writerPoolNil", writerPool == nil))

	if config == nil {
		panic("config cannot be nil")
	}
	if dynamoClient == nil {
		panic("dynamoClient cannot be nil")
	}
	if firestoreClient == nil {
		panic("firestoreClient cannot be nil")
	}
	if checkpointMgr == nil {
		panic("checkpointMgr cannot be nil")
	}
	if logger == nil {
		panic("logger cannot be nil")
	}
	if writerPool == nil {
		panic("writerPool cannot be nil")
	}

	replicator := &Replicator{
		config:          config,
		pair:            pair,
		dynamoClient:    dynamoClient,
		firestoreClient: firestoreClient,
		checkpointMgr:   checkpointMgr,
		logger:          logger,
		converter:       converter.NewDocumentConverter(pair.Mapping, logger),
		writerPool:      writerPool, // Assign the writer pool
	}

	logger.Info("Replicator created successfully",
		zap.Bool("replicatorDynamoClientNil", replicator.dynamoClient == nil))

	return replicator
}

// Replicate performs live replication for a single database pair
func (r *Replicator) Replicate(ctx context.Context, resume bool) error {
	r.logger.Info("Starting live replication",
		zap.String("source", r.pair.Source.TableName),
		zap.String("target", fmt.Sprintf("%s.%s", r.pair.Target.Database, r.pair.Target.Collection)),
		zap.Bool("resume_flag", resume))

	// Converter is already initialized in NewReplicator

	// ALWAYS try to load existing checkpoint first (auto-resume)
	replicationCheckpoint, err := r.checkpointMgr.Load(r.pair.Source.TableName)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	var needsInitialMigration bool

	// Handle checkpoint mode mismatch - auto-convert migrate to live
	if replicationCheckpoint != nil && replicationCheckpoint.Mode != "live" {
		r.logger.Warn("Found existing checkpoint with wrong mode, creating new live checkpoint",
			zap.String("existingMode", replicationCheckpoint.Mode),
			zap.String("requiredMode", "live"),
			zap.String("tableName", r.pair.Source.TableName))

		// Delete the old checkpoint
		if err := r.checkpointMgr.Delete(r.pair.Source.TableName); err != nil {
			r.logger.Warn("Failed to delete old checkpoint", zap.Error(err))
		}

		// Force creation of new live checkpoint
		replicationCheckpoint = nil
	}

	if replicationCheckpoint == nil {
		// Fresh start - need to create checkpoint and run migration
		needsInitialMigration = true
		r.logger.Info("No checkpoint found, starting fresh live replication with initial migration")

		// Get initial timestamp for new replication
		startTimestamp, err := r.getInitialTimestamp(ctx)
		if err != nil {
			return fmt.Errorf("failed to get initial timestamp: %w", err)
		}

		// Get stream ARN
		r.logger.Info("About to call GetStreamArn",
			zap.String("tableName", r.pair.Source.TableName),
			zap.Bool("dynamoClientNil", r.dynamoClient == nil))

		// Additional safety check
		if r.dynamoClient == nil {
			return fmt.Errorf("dynamoClient is nil when trying to get stream ARN")
		}

		streamArn, err := r.dynamoClient.GetStreamArn(ctx, r.pair.Source.TableName)
		if err != nil {
			return fmt.Errorf("failed to get stream ARN: %w", err)
		}

		// Create new checkpoint with initialMigrationComplete = false
		replicationCheckpoint = r.checkpointMgr.CreateLiveCheckpoint(r.pair.Source.TableName, streamArn, startTimestamp)

		if err := r.checkpointMgr.Save(r.pair.Source.TableName, replicationCheckpoint); err != nil {
			return fmt.Errorf("failed to save initial checkpoint: %w", err)
		}
	} else {
		// Existing checkpoint found - check migration status
		needsInitialMigration = !replicationCheckpoint.InitialMigrationComplete

		if needsInitialMigration {
			r.logger.Info("Existing checkpoint found but initial migration incomplete, will resume migration",
				zap.Time("checkpointTime", replicationCheckpoint.StartTime),
				zap.Bool("initialMigrationComplete", replicationCheckpoint.InitialMigrationComplete))
		} else {
			r.logger.Info("Existing checkpoint found with completed initial migration, skipping to stream processing",
				zap.Time("lastTimestamp", *replicationCheckpoint.Timestamp),
				zap.Bool("initialMigrationComplete", replicationCheckpoint.InitialMigrationComplete))
		}

		// Validate checkpoint is for live mode
		if replicationCheckpoint.Mode != "live" {
			return fmt.Errorf("existing checkpoint is for mode '%s', not 'live'", replicationCheckpoint.Mode)
		}

		// Validate stream ARN still matches (optional validation)
		r.logger.Debug("Checking r.dynamoClient before stream ARN validation", zap.Bool("isDynamoClientNil", r.dynamoClient == nil))
		if r.dynamoClient == nil {
			return fmt.Errorf("dynamoClient is nil when trying to validate stream ARN")
		}
		currentStreamArn, err := r.dynamoClient.GetStreamArn(ctx, r.pair.Source.TableName)
		if err != nil {
			r.logger.Warn("Failed to validate current stream ARN", zap.Error(err))
		} else if replicationCheckpoint.StreamArn != currentStreamArn {
			r.logger.Warn("Stream ARN changed since last checkpoint, updating checkpoint",
				zap.String("checkpointArn", replicationCheckpoint.StreamArn),
				zap.String("currentArn", currentStreamArn))
			// Update checkpoint with new stream ARN
			replicationCheckpoint.StreamArn = currentStreamArn
			if err := r.checkpointMgr.Save(r.pair.Source.TableName, replicationCheckpoint); err != nil {
				r.logger.Warn("Failed to update checkpoint with new stream ARN", zap.Error(err))
			}
		}
	}

	// Run initial migration if needed
	if needsInitialMigration {
		r.logger.Info("Starting initial migration for live replication")

		migrator := &Migrator{
			config:          r.config,
			pair:            r.pair,
			dynamoClient:    r.dynamoClient,
			firestoreClient: r.firestoreClient,
			checkpointMgr:   r.checkpointMgr,
			logger:          r.logger,
			writerPool:      r.writerPool, // Pass the writer pool
		}

		if err := migrator.MigrateSimple(ctx); err != nil {
			return fmt.Errorf("initial migration failed: %w", err)
		}

		// Mark initial migration as complete
		replicationCheckpoint.InitialMigrationComplete = true
		if err := r.checkpointMgr.Save(r.pair.Source.TableName, replicationCheckpoint); err != nil {
			r.logger.Warn("Failed to save initial migration completion status", zap.Error(err))
		}

		r.logger.Info("Initial migration completed successfully, proceeding to stream processing")
	}

	// Start stream processing
	return r.processStreams(ctx, replicationCheckpoint)
}

// getInitialTimestamp gets the initial timestamp for live replication
func (r *Replicator) getInitialTimestamp(ctx context.Context) (*time.Time, error) {
	// Return current timestamp as the starting point
	now := time.Now()
	return &now, nil
}

// processStreams processes DynamoDB streams for live replication with sequential shard processing
func (r *Replicator) processStreams(ctx context.Context, replicationCheckpoint *checkpoint.Checkpoint) error {
	r.logger.Info("Starting sequential stream processing",
		zap.String("streamArn", replicationCheckpoint.StreamArn),
		zap.Time("startTimestamp", *replicationCheckpoint.Timestamp))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get all current shards
		shardIDs, err := r.dynamoClient.GetStreamShards(ctx, replicationCheckpoint.StreamArn)
		if err != nil {
			r.logger.Error("Failed to get stream shards", zap.Error(err))
			time.Sleep(10 * time.Second)
			continue
		}

		if len(shardIDs) == 0 {
			r.logger.Info("No shards found in the stream, waiting...")
			time.Sleep(30 * time.Second)
			continue
		}

		// Sort shards by sequence number for chronological order
		orderedShardIDs := r.sortShardsBySequenceNumber(ctx, replicationCheckpoint.StreamArn, shardIDs)

		r.logger.Info("Processing all shards in chronological order",
			zap.Strings("shardOrder", orderedShardIDs),
			zap.Time("checkpointTimestamp", *replicationCheckpoint.Timestamp))

		// Process each shard sequentially
		// Timestamp filtering will automatically skip already-processed records
		for _, shardID := range orderedShardIDs {
			r.logger.Debug("Processing shard", zap.String("shardId", shardID))

			if err := r.processShardSequentially(ctx, replicationCheckpoint, shardID); err != nil {
				r.logger.Warn("Shard processing encountered error",
					zap.String("shardId", shardID), zap.Error(err))
				// Continue with next shard - don't fail the entire process
				continue
			}
		}

		// Wait before next discovery cycle
		r.logger.Debug("Completed shard processing cycle, waiting before next discovery")
		time.Sleep(30 * time.Second)
	}
}

// processShards processes multiple shards in parallel
func (r *Replicator) processShards(ctx context.Context, replicationCheckpoint *checkpoint.Checkpoint, shardIDs []string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(shardIDs))

	r.logger.Info("Starting parallel shard processing",
		zap.Int("shards", len(shardIDs)),
		zap.String("concurrency", "unlimited - all shards process simultaneously"))

	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()

			if err := r.processShard(ctx, replicationCheckpoint, shard); err != nil {
				errChan <- fmt.Errorf("shard %s failed: %w", shard, err)
			}
		}(shardID)
	}

	// Monitor for completion or errors
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Handle errors (but continue processing other shards)
	for err := range errChan {
		r.logger.Error("Shard processing error", zap.Error(err))
		// In live replication, we log errors but continue with other shards
	}

	return nil
}

// processShard processes a single shard
func (r *Replicator) processShard(ctx context.Context, replicationCheckpoint *checkpoint.Checkpoint, shardID string) error {
	shardLogger := r.logger.With(zap.String("shard", shardID))
	shardLogger.Info("Starting shard processing")

	// Always start from TRIM_HORIZON and filter by timestamp for simplicity
	shardLogger.Info("Starting shard from TRIM_HORIZON (will filter by timestamp)",
		zap.Time("filterAfter", *replicationCheckpoint.Timestamp))
	shardIterator, err := r.dynamoClient.GetShardIterator(ctx, replicationCheckpoint.StreamArn, shardID, "TRIM_HORIZON", nil)

	if err != nil {
		return fmt.Errorf("failed to get shard iterator: %w", err)
	}

	currentIterator := shardIterator
	processedRecords := 0

	for currentIterator != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get records from stream
		records, nextIterator, err := r.dynamoClient.GetStreamRecords(ctx, currentIterator)
		if err != nil {
			// Handle TrimmedDataAccessException or expired iterator
			if r.isTrimmedDataAccessException(err) {
				shardLogger.Warn("TrimmedDataAccessException: The operation attempted to read past the oldest stream record in a shard. Getting new iterator from LATEST.", zap.Error(err))
				// Get a new iterator from LATEST and continue
				newIterator, getErr := r.dynamoClient.GetShardIterator(ctx, replicationCheckpoint.StreamArn, shardID, "LATEST", nil)
				if getErr != nil {
					return fmt.Errorf("failed to get LATEST shard iterator after TrimmedDataAccessException: %w", getErr)
				}
				currentIterator = newIterator
				continue // Continue the loop with the new iterator
			} else if r.isResourceNotFoundError(err) {
				shardLogger.Info("Shard iterator expired or invalid, stopping processing")
				break
			}
			return fmt.Errorf("failed to get stream records: %w", err)
		}

		if len(records) > 0 {
			// Filter records by timestamp (same logic as Node.js)
			filteredRecords := r.filterRecordsByTimestamp(records, replicationCheckpoint.Timestamp)

			if len(filteredRecords) > 0 {
				shardLogger.Info("Processing filtered records",
					zap.Int("filtered", len(filteredRecords)),
					zap.Int("total", len(records)))

				if err := r.processRecords(ctx, filteredRecords, replicationCheckpoint, shardLogger, shardID); err != nil {
					return fmt.Errorf("failed to process records: %w", err)
				}

				processedRecords += len(filteredRecords)
			}
		}

		// Handle iterator continuation for infinite polling
		if nextIterator == "" {
			// No more records currently available, but keep polling for live replication
			shardLogger.Debug("No more records currently available, continuing to poll...")
			time.Sleep(5 * time.Second) // Longer wait when no records

			// Get a fresh iterator from current position to continue polling
			newIterator, getErr := r.dynamoClient.GetShardIterator(ctx, replicationCheckpoint.StreamArn, shardID, "LATEST", nil)
			if getErr != nil {
				shardLogger.Warn("Failed to get fresh iterator, retrying...", zap.Error(getErr))
				time.Sleep(10 * time.Second)
				continue
			}
			currentIterator = newIterator
		} else {
			// Normal case: more records available
			time.Sleep(1 * time.Second) // Normal polling interval
			currentIterator = nextIterator
		}
	}

	shardLogger.Info("Shard processing completed", zap.Int("processed", processedRecords))
	return nil
}

// filterRecordsByTimestamp filters records by timestamp (same logic as Node.js)
func (r *Replicator) filterRecordsByTimestamp(records []dynamodb.StreamRecord, startTimestamp *time.Time) []dynamodb.StreamRecord {
	if startTimestamp == nil {
		return records
	}

	var filteredRecords []dynamodb.StreamRecord
	startTimestampUnix := startTimestamp.UnixMilli()

	for _, record := range records {
		if record.DynamoDB != nil && record.DynamoDB.ApproximateCreationDateTime != nil {
			// Convert to milliseconds for comparison (same as Node.js)
			recordTimestamp := record.DynamoDB.ApproximateCreationDateTime.UnixMilli()

			// Same filtering logic as Node.js
			shouldInclude := recordTimestamp >= startTimestampUnix

			if shouldInclude {
				filteredRecords = append(filteredRecords, record)
			}
		}
	}

	return filteredRecords
}

// processRecords processes a batch of stream records
func (r *Replicator) processRecords(ctx context.Context, records []dynamodb.StreamRecord, replicationCheckpoint *checkpoint.Checkpoint, logger *zap.Logger, shardID string) error {
	pairConfig := r.config.GetPairConfig(r.pair.Name)
	batchSize := pairConfig.BatchSize

	// Process records in batches
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		if err := r.processRecordBatch(ctx, batch, logger); err != nil {
			return fmt.Errorf("batch processing failed: %w", err)
		}

		// Save checkpoint periodically with timestamp
		if (i+len(batch))%r.config.Migration.CheckpointFrequency == 0 {
			// Update checkpoint timestamp with the latest processed record's timestamp
			if len(batch) > 0 {
				lastRecord := batch[len(batch)-1]
				if lastRecord.DynamoDB != nil && lastRecord.DynamoDB.ApproximateCreationDateTime != nil {
					// Update timestamp with the record's creation time
					r.checkpointMgr.UpdateTimestamp(replicationCheckpoint, lastRecord.DynamoDB.ApproximateCreationDateTime)
				}
			}

			if err := r.checkpointMgr.Save(r.pair.Source.TableName, replicationCheckpoint); err != nil {
				logger.Warn("Failed to save checkpoint", zap.Error(err))
			}
		}
	}

	return nil
}

// processRecordBatch processes a batch of records with retry logic
func (r *Replicator) processRecordBatch(ctx context.Context, records []dynamodb.StreamRecord, logger *zap.Logger) error {
	var operations []firestore.Operation

	// Convert stream records to Firestore operations (same logic as Node.js)
	for _, record := range records {
		var operation firestore.Operation

		switch record.EventName {
		case "REMOVE":
			// Handle DELETE - use proper primary key mapping
			if record.DynamoDB.Keys != nil {
				deleteFilter := r.createDeleteFilter(record.DynamoDB.Keys)
				if deleteFilter != nil {
					operation = &firestore.DeleteOperation{
						Filter: deleteFilter,
					}
				} else {
					logger.Warn("Failed to create delete filter for REMOVE event")
					continue
				}
			}

		case "INSERT", "MODIFY":
			// Handle INSERT/UPDATE - same as Node.js (both use upsert)
			if record.DynamoDB.NewImage != nil {
				firestoreDoc, convertErr := r.converter.ConvertDocument(record.DynamoDB.NewImage)
				if convertErr != nil {
					logger.Warn("Failed to convert new image", zap.Error(convertErr))
					continue
				}
				operation = &firestore.UpsertOperation{
					Document: firestoreDoc,
					Filter:   nil, // Will use _id from document
				}
			}
		}

		if operation != nil {
			operations = append(operations, operation)
		}
	}

	if len(operations) == 0 {
		return nil
	}

	// Submit to writer pool
	job := firestore.WriteJob{
		Database:   r.pair.Target.Database,
		Collection: r.pair.Target.Collection,
		Operations: operations,
		ResultChan: make(chan error, 1), // Buffered channel for the result
	}

	r.writerPool.Submit(job)

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

// isResourceNotFoundError checks if the error is a resource not found error
func (r *Replicator) isResourceNotFoundError(err error) bool {
	// TODO: Implement proper error checking for DynamoDB Streams
	return err != nil && (strings.Contains(err.Error(), "ResourceNotFoundException") ||
		strings.Contains(err.Error(), "ExpiredIteratorException"))
}

// isTrimmedDataAccessException checks if the error is a TrimmedDataAccessException
func (r *Replicator) isTrimmedDataAccessException(err error) bool {
	return err != nil && strings.Contains(err.Error(), "TrimmedDataAccessException")
}

// isDuplicateKeyError checks if the error is a duplicate key error
func (r *Replicator) isDuplicateKeyError(err error) bool {
	// TODO: Implement proper error checking for MongoDB duplicate key errors
	return err != nil && (err.Error() == "E11000" ||
		err.Error() == "duplicate key error")
}

// ShardInfo represents shard information for sorting
type ShardInfo struct {
	ShardID                string
	StartingSequenceNumber *string
}

// sortShardsBySequenceNumber sorts shards by their starting sequence number for chronological order
func (r *Replicator) sortShardsBySequenceNumber(ctx context.Context, streamArn string, shardIDs []string) []string {
	// Get detailed shard information
	shards, err := r.getShardDetails(ctx, streamArn)
	if err != nil {
		r.logger.Warn("Failed to get shard details for sorting, using original order", zap.Error(err))
		return shardIDs
	}

	// Create a map for quick lookup
	shardMap := make(map[string]*ShardInfo)
	for _, shard := range shards {
		shardMap[shard.ShardID] = shard
	}

	// Filter and sort the requested shards
	var validShards []*ShardInfo
	for _, shardID := range shardIDs {
		if shard, exists := shardMap[shardID]; exists {
			validShards = append(validShards, shard)
		}
	}

	// Sort by starting sequence number
	for i := 0; i < len(validShards)-1; i++ {
		for j := i + 1; j < len(validShards); j++ {
			seq1 := validShards[i].StartingSequenceNumber
			seq2 := validShards[j].StartingSequenceNumber

			if seq1 == nil && seq2 != nil {
				// seq1 is nil, seq2 is not nil, swap
				validShards[i], validShards[j] = validShards[j], validShards[i]
			} else if seq1 != nil && seq2 != nil {
				// Both are not nil, compare numerically
				num1, err1 := parseSequenceNumber(*seq1)
				num2, err2 := parseSequenceNumber(*seq2)

				if err1 == nil && err2 == nil && num1 > num2 {
					validShards[i], validShards[j] = validShards[j], validShards[i]
				}
			}
		}
	}

	// Extract sorted shard IDs
	var orderedShardIDs []string
	for _, shard := range validShards {
		orderedShardIDs = append(orderedShardIDs, shard.ShardID)
	}

	return orderedShardIDs
}

// getShardDetails gets detailed information about shards including sequence numbers
func (r *Replicator) getShardDetails(ctx context.Context, streamArn string) ([]*ShardInfo, error) {
	// Use the enhanced DynamoDB client to get detailed shard information
	shardDetails, err := r.dynamoClient.GetStreamShardsWithDetails(ctx, streamArn)
	if err != nil {
		return nil, err
	}

	var shards []*ShardInfo
	for _, detail := range shardDetails {
		shards = append(shards, &ShardInfo{
			ShardID:                detail.ShardID,
			StartingSequenceNumber: detail.StartingSequenceNumber,
		})
	}

	return shards, nil
}

// parseSequenceNumber parses a sequence number string to int64
func parseSequenceNumber(seqNum string) (int64, error) {
	// DynamoDB sequence numbers are typically large integers as strings
	var result int64
	for _, char := range seqNum {
		if char >= '0' && char <= '9' {
			result = result*10 + int64(char-'0')
		}
	}
	return result, nil
}

// createDeleteFilter creates a proper delete filter using primary key mapping
func (r *Replicator) createDeleteFilter(keys map[string]interface{}) map[string]interface{} {
	if r.pair.Mapping.PrimaryKeyMapping != nil {
		sourceField := r.pair.Mapping.PrimaryKeyMapping.SourceField
		targetField := r.pair.Mapping.PrimaryKeyMapping.TargetField

		if keyValue, exists := keys[sourceField]; exists {
			convertedKey, err := r.converter.ConvertValue(sourceField, keyValue)
			if err == nil {
				r.logger.Debug("Created delete filter using primary key mapping",
					zap.String("sourceField", sourceField),
					zap.String("targetField", targetField),
					zap.Any("keyValue", convertedKey))
				return map[string]interface{}{targetField: convertedKey}
			} else {
				r.logger.Warn("Failed to convert primary key for delete filter",
					zap.String("sourceField", sourceField),
					zap.Error(err))
			}
		}
	}

	// Fallback to converted keys if primary key mapping fails or doesn't exist
	convertedKeys, err := r.converter.ConvertDocument(keys)
	if err != nil {
		r.logger.Warn("Failed to convert keys for delete filter", zap.Error(err))
		return nil
	}

	r.logger.Debug("Created delete filter using converted keys", zap.Any("filter", convertedKeys))
	return convertedKeys
}

// processShardSequentially processes a single shard sequentially
func (r *Replicator) processShardSequentially(ctx context.Context, replicationCheckpoint *checkpoint.Checkpoint, shardID string) error {
	shardLogger := r.logger.With(zap.String("shard", shardID))
	shardLogger.Info("Starting sequential shard processing")

	// Always start from TRIM_HORIZON and filter by timestamp for simplicity
	shardLogger.Info("Starting shard from TRIM_HORIZON (will filter by timestamp)",
		zap.Time("filterAfter", *replicationCheckpoint.Timestamp))
	shardIterator, err := r.dynamoClient.GetShardIterator(ctx, replicationCheckpoint.StreamArn, shardID, "TRIM_HORIZON", nil)

	if err != nil {
		// Handle common errors gracefully
		if r.isTrimmedDataAccessException(err) {
			shardLogger.Info("Shard data trimmed, skipping", zap.String("shardId", shardID))
			return nil
		}
		return fmt.Errorf("failed to get shard iterator: %w", err)
	}

	currentIterator := shardIterator
	processedRecords := 0

	for currentIterator != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get records from stream
		records, nextIterator, err := r.dynamoClient.GetStreamRecords(ctx, currentIterator)
		if err != nil {
			// Handle TrimmedDataAccessException or expired iterator
			if r.isTrimmedDataAccessException(err) {
				shardLogger.Info("Shard data trimmed, ending processing", zap.String("shardId", shardID))
				break
			} else if r.isResourceNotFoundError(err) {
				shardLogger.Info("Shard iterator expired or invalid, stopping processing")
				break
			}
			return fmt.Errorf("failed to get stream records: %w", err)
		}

		if len(records) > 0 {
			// Filter records by timestamp
			filteredRecords := r.filterRecordsByTimestamp(records, replicationCheckpoint.Timestamp)

			if len(filteredRecords) > 0 {
				shardLogger.Debug("Processing filtered records",
					zap.Int("filtered", len(filteredRecords)),
					zap.Int("total", len(records)))

				if err := r.processRecords(ctx, filteredRecords, replicationCheckpoint, shardLogger, shardID); err != nil {
					return fmt.Errorf("failed to process records: %w", err)
				}

				processedRecords += len(filteredRecords)
			}
		}

		// Continue to next batch
		if nextIterator == "" {
			shardLogger.Debug("Reached end of shard data",
				zap.String("shardId", shardID),
				zap.Int("recordsProcessed", processedRecords))
			break
		}

		currentIterator = nextIterator
		time.Sleep(100 * time.Millisecond) // Small delay between batches
	}

	shardLogger.Debug("Sequential shard processing completed",
		zap.String("shardId", shardID),
		zap.Int("processed", processedRecords))
	return nil
}
