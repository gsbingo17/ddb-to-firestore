package firestore

import (
	"context"
	"fmt"
	"time"

	"ddb-to-firestore/internal/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"
)

// Client wraps MongoDB client and provides database operations
type Client struct {
	client *mongo.Client
	config config.FirestoreConfig
	logger *zap.Logger
}

// Operation represents a MongoDB operation
type Operation interface {
	Execute(ctx context.Context, collection *mongo.Collection) error
}

// UpsertOperation represents an upsert operation
type UpsertOperation struct {
	Document map[string]interface{}
	Filter   map[string]interface{}
}

// DeleteOperation represents a delete operation
type DeleteOperation struct {
	Filter map[string]interface{}
}

// CollectionInfo contains information about a MongoDB collection
type CollectionInfo struct {
	Name          string
	DocumentCount int64
}

// NewClient creates a new Firestore client
func NewClient(cfg config.FirestoreConfig, logger *zap.Logger) (*Client, error) {
	// Create client options
	clientOpts := options.Client().ApplyURI(cfg.ConnectionString)

	// Set read preference
	if cfg.ReadPreference != "" {
		readPref, err := parseReadPreference(cfg.ReadPreference)
		if err != nil {
			return nil, fmt.Errorf("invalid read preference: %w", err)
		}
		clientOpts.SetReadPreference(readPref)
	}

	// Set write concern
	if cfg.WriteConcern != nil {
		wc, err := parseWriteConcern(cfg.WriteConcern)
		if err != nil {
			return nil, fmt.Errorf("invalid write concern: %w", err)
		}
		clientOpts.SetWriteConcern(wc)
	}

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	logger.Info("Connected to MongoDB", zap.String("uri", maskConnectionString(cfg.ConnectionString)))

	return &Client{
		client: client,
		config: cfg,
		logger: logger,
	}, nil
}

// Close closes the MongoDB client connection
func (c *Client) Close() error {
	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return c.client.Disconnect(ctx)
	}
	return nil
}

// Ping tests the connection to MongoDB
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

// ValidateCollection validates that the collection can be accessed
func (c *Client) ValidateCollection(ctx context.Context, database, collection string) error {
	db := c.client.Database(database)

	// Validate that the database exists and is accessible by listing collections.
	// Firestore's MongoDB compatibility layer does not support 'collStats'.
	_, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to list collections in database %s: %w", database, err)
	}

	// Firestore's MongoDB API does not support creating indexes.
	// We will skip explicit write access validation via index creation.
	// Actual write operations will validate write access during runtime.

	return nil
}

// GetCollectionInfo gets information about a MongoDB collection
func (c *Client) GetCollectionInfo(ctx context.Context, database, collection string) (*CollectionInfo, error) {
	db := c.client.Database(database)
	coll := db.Collection(collection)

	info := &CollectionInfo{
		Name: collection,
	}

	// Get document count using CountDocuments, as collStats is not supported by Firestore.
	// StorageSize is not available via this method and will not be displayed.
	docCount, err := coll.CountDocuments(ctx, bson.M{}) // Count all documents
	if err != nil {
		if isNamespaceNotFoundError(err) {
			// Collection doesn't exist yet
			return info, nil
		}
		return nil, fmt.Errorf("failed to get document count: %w", err)
	}
	info.DocumentCount = docCount

	return info, nil
}

// ExecuteBatch executes a batch of operations on a collection
func (c *Client) ExecuteBatch(ctx context.Context, database, collection string, operations []Operation) error {
	if len(operations) == 0 {
		return nil
	}

	db := c.client.Database(database)
	coll := db.Collection(collection)

	// Execute operations in a batch
	var models []mongo.WriteModel
	for _, op := range operations {
		switch operation := op.(type) {
		case *UpsertOperation:
			filter := operation.Filter
			if filter == nil {
				// If no filter specified, use _id from document
				if id, exists := operation.Document["_id"]; exists {
					filter = bson.M{"_id": id}
				} else {
					// No filter and no _id, treat as insert
					model := mongo.NewInsertOneModel().SetDocument(operation.Document)
					models = append(models, model)
					continue
				}
			}

			model := mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(operation.Document).
				SetUpsert(true)
			models = append(models, model)

		case *DeleteOperation:
			model := mongo.NewDeleteOneModel().SetFilter(operation.Filter)
			models = append(models, model)

		default:
			return fmt.Errorf("unsupported operation type: %T", op)
		}
	}

	if len(models) == 0 {
		return nil
	}

	// Execute bulk write
	opts := options.BulkWrite().SetOrdered(false) // Allow parallel execution
	result, err := coll.BulkWrite(ctx, models, opts)
	if err != nil {
		return fmt.Errorf("bulk write failed: %w", err)
	}

	c.logger.Debug("Bulk write completed",
		zap.String("database", database),
		zap.String("collection", collection),
		zap.Int("operations", len(operations)),
		zap.Int64("inserted", result.InsertedCount),
		zap.Int64("modified", result.ModifiedCount),
		zap.Int64("upserted", result.UpsertedCount),
		zap.Int64("deleted", result.DeletedCount))

	return nil
}

// UpsertDocument upserts a single document
func (c *Client) UpsertDocument(ctx context.Context, database, collection string, filter, document map[string]interface{}) error {
	db := c.client.Database(database)
	coll := db.Collection(collection)

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, filter, document, opts)
	if err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	return nil
}

// DeleteDocument deletes a single document
func (c *Client) DeleteDocument(ctx context.Context, database, collection string, filter map[string]interface{}) error {
	db := c.client.Database(database)
	coll := db.Collection(collection)

	_, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	return nil
}

// CreateIndex creates an index on a collection
func (c *Client) CreateIndex(ctx context.Context, database, collection string, keys bson.D, options *options.IndexOptions) error {
	db := c.client.Database(database)
	coll := db.Collection(collection)

	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: options,
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

// Execute implements Operation interface for UpsertOperation
func (op *UpsertOperation) Execute(ctx context.Context, collection *mongo.Collection) error {
	filter := op.Filter
	if filter == nil {
		if id, exists := op.Document["_id"]; exists {
			filter = bson.M{"_id": id}
		} else {
			// No filter and no _id, treat as insert
			_, err := collection.InsertOne(ctx, op.Document)
			return err
		}
	}

	opts := options.Replace().SetUpsert(true)
	_, err := collection.ReplaceOne(ctx, filter, op.Document, opts)
	return err
}

// Execute implements Operation interface for DeleteOperation
func (op *DeleteOperation) Execute(ctx context.Context, collection *mongo.Collection) error {
	_, err := collection.DeleteOne(ctx, op.Filter)
	return err
}

// Helper functions

// parseReadPreference parses a read preference string
func parseReadPreference(pref string) (*readpref.ReadPref, error) {
	switch pref {
	case "primary":
		return readpref.Primary(), nil
	case "primaryPreferred":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondaryPreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("invalid read preference: %s", pref)
	}
}

// parseWriteConcern parses write concern configuration
func parseWriteConcern(wc *config.WriteConcernConfig) (*writeconcern.WriteConcern, error) {
	var opts []writeconcern.Option

	switch v := wc.W.(type) {
	case string:
		if v == "majority" {
			opts = append(opts, writeconcern.WMajority())
		} else {
			opts = append(opts, writeconcern.WTagSet(v))
		}
	case int:
		opts = append(opts, writeconcern.W(v))
	case float64:
		opts = append(opts, writeconcern.W(int(v)))
	default:
		return nil, fmt.Errorf("invalid write concern W value: %v", wc.W)
	}

	if wc.J {
		opts = append(opts, writeconcern.J(true))
	}

	if wc.WTimeout > 0 {
		opts = append(opts, writeconcern.WTimeout(time.Duration(wc.WTimeout)*time.Millisecond))
	}

	return writeconcern.New(opts...), nil
}

// maskConnectionString masks sensitive information in connection string
func maskConnectionString(connStr string) string {
	if len(connStr) < 20 {
		return "***"
	}
	return connStr[:10] + "***" + connStr[len(connStr)-10:]
}

// isNamespaceNotFoundError checks if the error is a namespace not found error
func isNamespaceNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if cmdErr, ok := err.(mongo.CommandError); ok {
		return cmdErr.Code == 26 // NamespaceNotFound
	}

	return false
}
