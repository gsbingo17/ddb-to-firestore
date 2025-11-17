package firestore

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"go.uber.org/zap"

	"ddb-to-firestore/internal/config"
)

// Client wraps Firestore client and provides database operations
type Client struct {
	client    *firestore.Client
	projectID string
	config    config.FirestoreConfig
	logger    *zap.Logger
}

// Operation represents a Firestore operation
type Operation interface {
	Execute(ctx context.Context, batch *firestore.WriteBatch, collection *firestore.CollectionRef) error
}

// UpsertOperation represents a document set operation
type UpsertOperation struct {
	DocumentID  string                 // Firestore document ID (empty string = auto-generate)
	Document    map[string]interface{} // Document data
	OriginalKey map[string]interface{} // DynamoDB original key to store
}

// DeleteOperation represents a document delete operation
type DeleteOperation struct {
	DocumentID string // Firestore document ID to delete
}

// CollectionInfo contains information about a Firestore collection
type CollectionInfo struct {
	Name          string
	DocumentCount int64
}

// NewClient creates a new Firestore client
func NewClient(cfg config.FirestoreConfig, logger *zap.Logger) (*Client, error) {
	ctx := context.Background()

	// Set timeout for client creation
	if cfg.Timeouts != nil && cfg.Timeouts.ConnectionTimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx,
			time.Duration(cfg.Timeouts.ConnectionTimeoutMs)*time.Millisecond)
		defer cancel()
	}

	// Create Firestore client with credentials
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	// Specify database if not default
	databaseID := cfg.DatabaseID
	if databaseID == "" {
		databaseID = "(default)"
	}

	client, err := firestore.NewClientWithDatabase(ctx, cfg.ProjectID, databaseID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}

	logger.Info("Connected to Firestore",
		zap.String("projectId", cfg.ProjectID),
		zap.String("databaseId", databaseID))

	return &Client{
		client:    client,
		projectID: cfg.ProjectID,
		config:    cfg,
		logger:    logger,
	}, nil
}

// Close closes the Firestore client connection
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// ExecuteBatch executes a batch of operations on a collection
func (c *Client) ExecuteBatch(ctx context.Context, database, collection string, operations []Operation) error {
	if len(operations) == 0 {
		return nil
	}

	// Firestore batch limit is 500 operations
	const maxBatchSize = 500

	for i := 0; i < len(operations); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(operations) {
			end = len(operations)
		}

		batch := operations[i:end]
		if err := c.executeBatchChunk(ctx, collection, batch); err != nil {
			return fmt.Errorf("batch chunk %d-%d failed: %w", i, end, err)
		}
	}

	return nil
}

// executeBatchChunk executes a single batch chunk (max 500 operations)
func (c *Client) executeBatchChunk(ctx context.Context, collectionName string, operations []Operation) error {
	batch := c.client.Batch()
	coll := c.client.Collection(collectionName)

	var upsertCount, deleteCount int

	for _, op := range operations {
		if err := op.Execute(ctx, batch, coll); err != nil {
			return fmt.Errorf("failed to add operation to batch: %w", err)
		}

		switch op.(type) {
		case *UpsertOperation:
			upsertCount++
		case *DeleteOperation:
			deleteCount++
		}
	}

	// Commit the batch
	_, err := batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("batch commit failed: %w", err)
	}

	c.logger.Debug("Batch write completed",
		zap.String("collection", collectionName),
		zap.Int("totalOps", len(operations)),
		zap.Int("upserts", upsertCount),
		zap.Int("deletes", deleteCount))

	return nil
}

// GetCollectionInfo gets information about a Firestore collection
func (c *Client) GetCollectionInfo(ctx context.Context, database, collection string) (*CollectionInfo, error) {
	coll := c.client.Collection(collection)

	// Count documents
	docCount := int64(0)
	iter := coll.Documents(ctx)
	defer iter.Stop()

	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate documents: %w", err)
		}
		docCount++
	}

	return &CollectionInfo{
		Name:          collection,
		DocumentCount: docCount,
	}, nil
}

// ValidateCollection validates that the collection can be accessed
func (c *Client) ValidateCollection(ctx context.Context, database, collection string) error {
	coll := c.client.Collection(collection)

	// Try to get collection reference (doesn't require collection to exist)
	if coll == nil {
		return fmt.Errorf("failed to get collection reference: %s", collection)
	}

	return nil
}

// Ping tests the connection to Firestore
func (c *Client) Ping(ctx context.Context) error {
	// Try to list collections as a ping test
	iter := c.client.Collections(ctx)
	_, err := iter.Next()
	if err != nil && err != iterator.Done {
		return fmt.Errorf("firestore ping failed: %w", err)
	}
	return nil
}

// FindDocumentByKey finds a document by its DynamoDB key components
func (c *Client) FindDocumentByKey(ctx context.Context, collectionName string, partitionKey, sortKey interface{}, mapping *config.PrimaryKeyMapping) (string, error) {
	coll := c.client.Collection(collectionName)

	// Build query based on key type
	var query firestore.Query

	if mapping.CompositeKeyField != "" && sortKey != nil {
		// Use composite key field for faster single-field query
		delimiter := mapping.Delimiter
		if delimiter == "" {
			delimiter = "#"
		}
		compositeKey := fmt.Sprintf("%v%s%v", partitionKey, delimiter, sortKey)
		query = coll.Where(mapping.CompositeKeyField, "==", compositeKey)
	} else if sortKey != nil && mapping.SortKey != nil {
		// Composite key: query with both partition and sort key
		query = coll.Where(mapping.PartitionKey.TargetField, "==", partitionKey).
			Where(mapping.SortKey.TargetField, "==", sortKey)
	} else {
		// Simple key: query with partition key only
		query = coll.Where(mapping.PartitionKey.TargetField, "==", partitionKey)
	}

	// Execute query
	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return "", fmt.Errorf("query failed: %w", err)
	}

	if len(docs) == 0 {
		return "", fmt.Errorf("document not found")
	}

	if len(docs) > 1 {
		c.logger.Warn("Multiple documents found for key, using first",
			zap.Int("count", len(docs)))
	}

	return docs[0].Ref.ID, nil
}

// Execute implements Operation interface for UpsertOperation
func (op *UpsertOperation) Execute(ctx context.Context, batch *firestore.WriteBatch, collection *firestore.CollectionRef) error {
	var docRef *firestore.DocumentRef

	if op.DocumentID != "" {
		// Use specified document ID
		docRef = collection.Doc(op.DocumentID)
	} else {
		// Auto-generate document ID
		docRef = collection.NewDoc()
	}

	batch.Set(docRef, op.Document)
	return nil
}

// Execute implements Operation interface for DeleteOperation
func (op *DeleteOperation) Execute(ctx context.Context, batch *firestore.WriteBatch, collection *firestore.CollectionRef) error {
	if op.DocumentID == "" {
		return fmt.Errorf("document ID is required for delete operation")
	}

	docRef := collection.Doc(op.DocumentID)
	batch.Delete(docRef)
	return nil
}

// IsNotFoundError checks if the error is a "not found" error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}

// maskConnectionString is a helper function (keeping for compatibility)
func maskConnectionString(connStr string) string {
	if len(connStr) < 20 {
		return "***"
	}
	return connStr[:10] + "***" + connStr[len(connStr)-10:]
}
