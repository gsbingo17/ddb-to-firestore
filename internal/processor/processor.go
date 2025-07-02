package processor

import (
	"context"
	"fmt"
	"sync"

	"ddb-to-firestore/internal/checkpoint"
	"ddb-to-firestore/internal/config"
	"ddb-to-firestore/internal/dynamodb"
	"ddb-to-firestore/internal/firestore"
	"ddb-to-firestore/internal/utils"

	"go.uber.org/zap"
)

// Processor coordinates the migration/replication process
type Processor struct {
	config          *config.Config
	logger          *zap.Logger
	dynamoClients   map[string]*dynamodb.Client
	firestoreClient *firestore.Client
	checkpointMgr   *checkpoint.Manager
	writerPool      *firestore.WriterPool // New field for the writer pool
	mu              sync.RWMutex
	closed          bool
}

// New creates a new processor instance
func New(cfg *config.Config, logger *zap.Logger) (*Processor, error) {
	// Initialize Firestore client
	firestoreClient, err := firestore.NewClient(cfg.Firestore, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}

	// Initialize checkpoint manager
	checkpointMgr, err := checkpoint.NewManager(cfg.Checkpoint, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
	}

	// Initialize DynamoDB clients for each unique region/endpoint
	dynamoClients := make(map[string]*dynamodb.Client)
	for _, pair := range cfg.DatabasePairs {
		key := fmt.Sprintf("%s:%s", pair.Source.Region, pair.Source.Endpoint)
		if _, exists := dynamoClients[key]; !exists {
			client, err := dynamodb.NewClient(pair.Source, logger)
			if err != nil {
				return nil, fmt.Errorf("failed to create DynamoDB client for %s: %w", key, err)
			}
			dynamoClients[key] = client
		}
	}

	// Initialize Firestore writer pool
	writerPool := firestore.NewWriterPool(firestoreClient, cfg.Parallelism.FirestoreWriters, logger)

	return &Processor{
		config:          cfg,
		logger:          logger,
		dynamoClients:   dynamoClients,
		firestoreClient: firestoreClient,
		checkpointMgr:   checkpointMgr,
		writerPool:      writerPool, // Assign the new writer pool
	}, nil
}

// Close closes all connections and resources
func (p *Processor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	var errs []error

	// Close Firestore client
	if p.firestoreClient != nil {
		if err := p.firestoreClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Firestore client: %w", err))
		}
	}

	// Close DynamoDB clients
	for key, client := range p.dynamoClients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close DynamoDB client %s: %w", key, err))
		}
	}

	// Close checkpoint manager
	if p.checkpointMgr != nil {
		if err := p.checkpointMgr.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close checkpoint manager: %w", err))
		}
	}

	// Close writer pool
	if p.writerPool != nil {
		p.writerPool.Close() // WriterPool has its own Close method
	}

	p.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// RunMigration runs one-time migration for all database pairs
func (p *Processor) RunMigration(ctx context.Context, resume, dryRun bool) error {
	p.logger.Info("Starting migration",
		zap.Int("pairs", len(p.config.DatabasePairs)),
		zap.Bool("resume", resume),
		zap.Bool("dryRun", dryRun))

	if dryRun {
		return p.runDryRun(ctx)
	}

	// Process pairs with concurrency control
	semaphore := make(chan struct{}, p.config.Parallelism.MaxConcurrentPairs)
	var wg sync.WaitGroup
	errChan := make(chan error, len(p.config.DatabasePairs))

	for _, pair := range p.config.DatabasePairs {
		wg.Add(1)
		go func(pair config.DatabasePair) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := p.migratePair(ctx, pair, resume); err != nil {
				errChan <- fmt.Errorf("failed to migrate pair %s: %w", pair.Name, err)
			}
		}(pair)
	}

	// Wait for all pairs to complete
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

	p.logger.Info("Migration completed successfully")
	return nil
}

// RunLiveReplication runs live replication for all database pairs
func (p *Processor) RunLiveReplication(ctx context.Context, resume, dryRun bool) error {
	p.logger.Info("Starting live replication",
		zap.Int("pairs", len(p.config.DatabasePairs)),
		zap.Bool("resume", resume),
		zap.Bool("dryRun", dryRun))

	if dryRun {
		return p.runDryRun(ctx)
	}

	// Process pairs with concurrency control
	semaphore := make(chan struct{}, p.config.Parallelism.MaxConcurrentPairs)
	var wg sync.WaitGroup
	errChan := make(chan error, len(p.config.DatabasePairs))

	for _, pair := range p.config.DatabasePairs {
		wg.Add(1)
		go func(pair config.DatabasePair) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := p.replicatePair(ctx, pair, resume); err != nil {
				errChan <- fmt.Errorf("failed to replicate pair %s: %w", pair.Name, err)
			}
		}(pair)
	}

	// Wait for all pairs to complete or context cancellation
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Monitor for errors
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				p.logger.Info("Live replication completed")
				return nil
			}
			if err != nil {
				p.logger.Error("Replication error", zap.Error(err))
				// Continue with other pairs
			}
		case <-ctx.Done():
			p.logger.Info("Live replication stopped", zap.Error(ctx.Err()))
			return ctx.Err()
		}
	}
}

// ShowStatus shows the current status of all database pairs
func (p *Processor) ShowStatus(ctx context.Context, pairNames []string) error {
	p.logger.Info("Checking status", zap.Strings("pairs", pairNames))

	pairs := p.config.DatabasePairs
	if len(pairNames) > 0 {
		pairs = p.filterPairs(pairs, pairNames)
	}

	for _, pair := range pairs {
		status, err := p.getPairStatus(ctx, pair)
		if err != nil {
			p.logger.Error("Failed to get status", zap.String("pair", pair.Name), zap.Error(err))
			continue
		}

		p.logger.Info("Pair status",
			zap.String("pair", pair.Name),
			zap.Any("status", status))
	}

	return nil
}

// HealthCheck performs health checks on all connections
func (p *Processor) HealthCheck(ctx context.Context) error {
	p.logger.Info("Performing health check")

	// Check Firestore connection
	if err := p.firestoreClient.Ping(ctx); err != nil {
		utils.LogConnectionStatus(p.logger, "firestore", "unhealthy", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("Firestore health check failed: %w", err)
	}
	utils.LogConnectionStatus(p.logger, "firestore", "healthy", nil)

	// Check DynamoDB connections
	for key, client := range p.dynamoClients {
		if err := client.Ping(ctx); err != nil {
			utils.LogConnectionStatus(p.logger, "dynamodb", "unhealthy", map[string]interface{}{
				"client": key,
				"error":  err.Error(),
			})
			return fmt.Errorf("DynamoDB health check failed for %s: %w", key, err)
		}
		utils.LogConnectionStatus(p.logger, "dynamodb", "healthy", map[string]interface{}{
			"client": key,
		})
	}

	p.logger.Info("Health check passed")
	return nil
}

// migratePair performs migration for a single database pair
func (p *Processor) migratePair(ctx context.Context, pair config.DatabasePair, resume bool) error {
	pairLogger, err := utils.NewPairLogger(p.config.Logging, pair.Name)
	if err != nil {
		return fmt.Errorf("failed to create pair logger: %w", err)
	}
	defer pairLogger.Sync()

	pairLogger.Info("Starting migration", zap.String("table", pair.Source.TableName))

	// Get DynamoDB client
	clientKey := fmt.Sprintf("%s:%s", pair.Source.Region, pair.Source.Endpoint)
	dynamoClient, exists := p.dynamoClients[clientKey]
	if !exists {
		return fmt.Errorf("DynamoDB client not found for %s", clientKey)
	}

	// Create migrator
	migrator := &Migrator{
		config:          p.config,
		pair:            pair,
		dynamoClient:    dynamoClient,
		firestoreClient: p.firestoreClient,
		checkpointMgr:   p.checkpointMgr,
		logger:          pairLogger,
		writerPool:      p.writerPool, // Pass the writer pool
	}

	return migrator.Migrate(ctx, resume)
}

// replicatePair performs live replication for a single database pair
func (p *Processor) replicatePair(ctx context.Context, pair config.DatabasePair, resume bool) error {
	pairLogger, err := utils.NewPairLogger(p.config.Logging, pair.Name)
	if err != nil {
		return fmt.Errorf("failed to create pair logger: %w", err)
	}
	defer pairLogger.Sync()

	pairLogger.Info("Starting live replication", zap.String("table", pair.Source.TableName))

	// Get DynamoDB client
	clientKey := fmt.Sprintf("%s:%s", pair.Source.Region, pair.Source.Endpoint)
	pairLogger.Info("Looking for DynamoDB client", zap.String("clientKey", clientKey))

	// Debug: log all available client keys
	availableKeys := make([]string, 0, len(p.dynamoClients))
	for key := range p.dynamoClients {
		availableKeys = append(availableKeys, key)
	}
	pairLogger.Info("Available DynamoDB client keys", zap.Strings("keys", availableKeys))

	dynamoClient, exists := p.dynamoClients[clientKey]
	if !exists {
		return fmt.Errorf("DynamoDB client not found for %s, available keys: %v", clientKey, availableKeys)
	}

	if dynamoClient == nil {
		return fmt.Errorf("DynamoDB client is nil for key %s", clientKey)
	}

	// Create replicator
	replicator := NewReplicator(p.config, pair, dynamoClient, p.firestoreClient, p.checkpointMgr, pairLogger, p.writerPool)

	return replicator.Replicate(ctx, resume)
}

// runDryRun performs a dry run validation
func (p *Processor) runDryRun(ctx context.Context) error {
	p.logger.Info("Running dry run validation")

	for _, pair := range p.config.DatabasePairs {
		p.logger.Info("Validating pair",
			zap.String("name", pair.Name),
			zap.String("source", pair.Source.TableName),
			zap.String("target", fmt.Sprintf("%s.%s", pair.Target.Database, pair.Target.Collection)))

		// Validate DynamoDB table exists
		clientKey := fmt.Sprintf("%s:%s", pair.Source.Region, pair.Source.Endpoint)
		dynamoClient := p.dynamoClients[clientKey]
		if err := dynamoClient.ValidateTable(ctx, pair.Source.TableName); err != nil {
			return fmt.Errorf("DynamoDB table validation failed for %s: %w", pair.Name, err)
		}

		// Validate Firestore collection access
		if err := p.firestoreClient.ValidateCollection(ctx, pair.Target.Database, pair.Target.Collection); err != nil {
			return fmt.Errorf("Firestore collection validation failed for %s: %w", pair.Name, err)
		}
	}

	p.logger.Info("Dry run validation completed successfully")
	return nil
}

// getPairStatus gets the current status of a database pair
func (p *Processor) getPairStatus(ctx context.Context, pair config.DatabasePair) (map[string]interface{}, error) {
	status := make(map[string]interface{})

	// Get checkpoint information
	checkpoint, err := p.checkpointMgr.Load(pair.Source.TableName)
	if err != nil {
		status["checkpoint_error"] = err.Error()
	} else {
		status["checkpoint"] = checkpoint
	}

	// Get source table info
	clientKey := fmt.Sprintf("%s:%s", pair.Source.Region, pair.Source.Endpoint)
	dynamoClient := p.dynamoClients[clientKey]
	tableInfo, err := dynamoClient.GetTableInfo(ctx, pair.Source.TableName)
	if err != nil {
		status["source_error"] = err.Error()
	} else {
		status["source"] = tableInfo
	}

	// Get target collection info
	collectionInfo, err := p.firestoreClient.GetCollectionInfo(ctx, pair.Target.Database, pair.Target.Collection)
	if err != nil {
		// Check if the error is due to unsupported collStats command (specific to Firestore's MongoDB API)
		if err.Error() == "failed to get collection stats: (InvalidArgument) Unsupported command collStats" {
			status["target_info_status"] = "unavailable (collStats not supported by Firestore)"
		} else {
			status["target_error"] = err.Error()
		}
	} else {
		status["target"] = collectionInfo
	}

	return status, nil
}

// filterPairs filters database pairs by names
func (p *Processor) filterPairs(allPairs []config.DatabasePair, names []string) []config.DatabasePair {
	nameSet := make(map[string]bool)
	for _, name := range names {
		nameSet[name] = true
	}

	var filtered []config.DatabasePair
	for _, pair := range allPairs {
		if nameSet[pair.Name] {
			filtered = append(filtered, pair)
		}
	}

	return filtered
}
