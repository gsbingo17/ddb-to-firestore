package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"ddb-to-firestore/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"go.uber.org/zap"
)

// Client wraps DynamoDB and DynamoDB Streams clients
type Client struct {
	dynamodb *dynamodb.Client
	streams  *dynamodbstreams.Client
	config   config.SourceConfig
	logger   *zap.Logger
}

// ScanResult represents a batch of scanned items
type ScanResult struct {
	Items            []map[string]interface{}
	LastEvaluatedKey map[string]interface{}
	Count            int32
	ScannedCount     int32
}

// StreamRecord represents a DynamoDB stream record
type StreamRecord struct {
	EventName   string
	DynamoDB    *StreamRecordData
	EventSource string
	AWSRegion   string
}

// StreamRecordData contains the DynamoDB-specific data
type StreamRecordData struct {
	ApproximateCreationDateTime *time.Time
	Keys                        map[string]interface{}
	NewImage                    map[string]interface{}
	OldImage                    map[string]interface{}
	SequenceNumber              string
	SizeBytes                   int64
	StreamViewType              string
}

// TableInfo contains information about a DynamoDB table
type TableInfo struct {
	TableName      string
	ItemCount      int64
	TableSizeBytes int64
	Status         string
	StreamArn      string
	StreamEnabled  bool
}

// NewClient creates a new DynamoDB client
func NewClient(cfg config.SourceConfig, logger *zap.Logger) (*Client, error) {
	// Load AWS configuration
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create DynamoDB client
	var dynamoClient *dynamodb.Client
	if cfg.Endpoint != "" {
		// Use custom endpoint (e.g., DynamoDB Local)
		dynamoClient = dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	} else {
		dynamoClient = dynamodb.NewFromConfig(awsCfg)
	}

	// Create DynamoDB Streams client
	var streamsClient *dynamodbstreams.Client
	if cfg.Endpoint != "" {
		// For DynamoDB Local, streams typically use the same endpoint
		streamsClient = dynamodbstreams.NewFromConfig(awsCfg, func(o *dynamodbstreams.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	} else {
		streamsClient = dynamodbstreams.NewFromConfig(awsCfg)
	}

	return &Client{
		dynamodb: dynamoClient,
		streams:  streamsClient,
		config:   cfg,
		logger:   logger,
	}, nil
}

// Close closes the client connections
func (c *Client) Close() error {
	// AWS SDK clients don't need explicit closing
	return nil
}

// Ping tests the connection to DynamoDB
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.dynamodb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.config.TableName),
	})
	return err
}

// ValidateTable validates that the table exists and is accessible
func (c *Client) ValidateTable(ctx context.Context, tableName string) error {
	_, err := c.dynamodb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("table validation failed: %w", err)
	}
	return nil
}

// GetTableInfo gets information about a DynamoDB table
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	result, err := c.dynamodb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	table := result.Table
	info := &TableInfo{
		TableName:     *table.TableName,
		Status:        string(table.TableStatus),
		StreamEnabled: table.StreamSpecification != nil && *table.StreamSpecification.StreamEnabled,
	}

	if table.ItemCount != nil {
		info.ItemCount = *table.ItemCount
	}

	if table.TableSizeBytes != nil {
		info.TableSizeBytes = *table.TableSizeBytes
	}

	if table.LatestStreamArn != nil {
		info.StreamArn = *table.LatestStreamArn
	}

	return info, nil
}

// Scan performs a DynamoDB scan operation
func (c *Client) Scan(ctx context.Context, segment, totalSegments int, lastEvaluatedKey map[string]interface{}) (*ScanResult, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(c.config.TableName),
	}

	// Add parallel scan parameters if specified
	if totalSegments > 0 {
		input.Segment = aws.Int32(int32(segment))
		input.TotalSegments = aws.Int32(int32(totalSegments))
	}

	// Add last evaluated key for pagination
	if lastEvaluatedKey != nil {
		dynamoKey, err := convertToAttributeValueMap(lastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to convert last evaluated key: %w", err)
		}
		input.ExclusiveStartKey = dynamoKey
	}

	// Add scan configuration if specified
	if c.config.ScanConfig != nil {
		scanCfg := c.config.ScanConfig

		if scanCfg.ConsistentRead {
			input.ConsistentRead = aws.Bool(true)
		}

		if scanCfg.ProjectionExpression != "" {
			input.ProjectionExpression = aws.String(scanCfg.ProjectionExpression)
		}

		if scanCfg.FilterExpression != "" {
			input.FilterExpression = aws.String(scanCfg.FilterExpression)
		}

		if len(scanCfg.ExpressionAttributeNames) > 0 {
			input.ExpressionAttributeNames = scanCfg.ExpressionAttributeNames
		}

		if len(scanCfg.ExpressionAttributeValues) > 0 {
			attrValues, err := convertToAttributeValueMap(scanCfg.ExpressionAttributeValues)
			if err != nil {
				return nil, fmt.Errorf("failed to convert expression attribute values: %w", err)
			}
			input.ExpressionAttributeValues = attrValues
		}
	}

	result, err := c.dynamodb.Scan(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Convert items to map[string]interface{}
	items := make([]map[string]interface{}, len(result.Items))
	for i, item := range result.Items {
		converted, err := convertFromAttributeValueMap(item)
		if err != nil {
			return nil, fmt.Errorf("failed to convert item %d: %w", i, err)
		}
		items[i] = converted
	}

	// Convert last evaluated key
	var lastKey map[string]interface{}
	if result.LastEvaluatedKey != nil {
		lastKey, err = convertFromAttributeValueMap(result.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to convert last evaluated key: %w", err)
		}
	}

	return &ScanResult{
		Items:            items,
		LastEvaluatedKey: lastKey,
		Count:            result.Count,
		ScannedCount:     result.ScannedCount,
	}, nil
}

// GetStreamArn gets the stream ARN for a table
func (c *Client) GetStreamArn(ctx context.Context, tableName string) (string, error) {
	c.logger.Info("Getting stream ARN", zap.String("tableName", tableName))

	if c.dynamodb == nil {
		return "", fmt.Errorf("DynamoDB client is nil")
	}

	result, err := c.dynamodb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to describe table: %w", err)
	}

	c.logger.Info("DescribeTable result received")

	if result == nil {
		return "", fmt.Errorf("DescribeTable returned nil result")
	}

	if result.Table == nil {
		return "", fmt.Errorf("DescribeTable returned nil table")
	}

	c.logger.Info("Checking stream ARN",
		zap.Bool("hasLatestStreamArn", result.Table.LatestStreamArn != nil),
		zap.Bool("hasStreamSpec", result.Table.StreamSpecification != nil))

	if result.Table.LatestStreamArn == nil {
		return "", fmt.Errorf("DynamoDB Streams is not enabled on table %s", tableName)
	}

	streamArn := *result.Table.LatestStreamArn
	c.logger.Info("Stream ARN found", zap.String("streamArn", streamArn))
	return streamArn, nil
}

// ShardDetail represents detailed information about a shard
type ShardDetail struct {
	ShardID                string
	StartingSequenceNumber *string
	EndingSequenceNumber   *string
	ParentShardID          *string
}

// GetStreamShards gets all shards for a stream
func (c *Client) GetStreamShards(ctx context.Context, streamArn string) ([]string, error) {
	result, err := c.streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}

	var shardIDs []string
	for _, shard := range result.StreamDescription.Shards {
		if shard.ShardId != nil {
			shardIDs = append(shardIDs, *shard.ShardId)
		}
	}

	return shardIDs, nil
}

// GetStreamShardsWithDetails gets detailed information about all shards for a stream
func (c *Client) GetStreamShardsWithDetails(ctx context.Context, streamArn string) ([]ShardDetail, error) {
	result, err := c.streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}

	var shards []ShardDetail
	for _, shard := range result.StreamDescription.Shards {
		if shard.ShardId != nil {
			shardDetail := ShardDetail{
				ShardID: *shard.ShardId,
			}

			if shard.ParentShardId != nil {
				shardDetail.ParentShardID = shard.ParentShardId
			}

			if shard.SequenceNumberRange != nil {
				shardDetail.StartingSequenceNumber = shard.SequenceNumberRange.StartingSequenceNumber
				shardDetail.EndingSequenceNumber = shard.SequenceNumberRange.EndingSequenceNumber
			}

			shards = append(shards, shardDetail)
		}
	}

	return shards, nil
}

// GetShardIterator gets a shard iterator for reading stream records
func (c *Client) GetShardIterator(ctx context.Context, streamArn, shardID string, iteratorType string, sequenceNumber *string) (string, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: streamtypes.ShardIteratorType(iteratorType),
	}

	if sequenceNumber != nil {
		input.SequenceNumber = sequenceNumber
	}

	result, err := c.streams.GetShardIterator(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get shard iterator: %w", err)
	}

	if result.ShardIterator == nil {
		return "", fmt.Errorf("received nil shard iterator")
	}

	return *result.ShardIterator, nil
}

// GetStreamRecords gets records from a shard iterator
func (c *Client) GetStreamRecords(ctx context.Context, shardIterator string) ([]StreamRecord, string, error) {
	result, err := c.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(shardIterator),
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to get records: %w", err)
	}

	records := make([]StreamRecord, len(result.Records))
	for i, record := range result.Records {
		converted, err := c.convertStreamRecord(record)
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert record %d: %w", i, err)
		}
		records[i] = *converted
	}

	nextIterator := ""
	if result.NextShardIterator != nil {
		nextIterator = *result.NextShardIterator
	}

	return records, nextIterator, nil
}

// convertStreamRecord converts a DynamoDB stream record to our internal format
func (c *Client) convertStreamRecord(record streamtypes.Record) (*StreamRecord, error) {
	streamRecord := &StreamRecord{
		EventName:   string(record.EventName),
		EventSource: aws.ToString(record.EventSource),
		AWSRegion:   aws.ToString(record.AwsRegion),
	}

	if record.Dynamodb != nil {
		dynamoData := &StreamRecordData{
			SequenceNumber: aws.ToString(record.Dynamodb.SequenceNumber),
			SizeBytes:      aws.ToInt64(record.Dynamodb.SizeBytes),
			StreamViewType: string(record.Dynamodb.StreamViewType),
		}

		if record.Dynamodb.ApproximateCreationDateTime != nil {
			dynamoData.ApproximateCreationDateTime = record.Dynamodb.ApproximateCreationDateTime
		}

		if record.Dynamodb.Keys != nil {
			keys, err := convertFromStreamAttributeValueMap(record.Dynamodb.Keys)
			if err != nil {
				return nil, fmt.Errorf("failed to convert keys: %w", err)
			}
			dynamoData.Keys = keys
		}

		if record.Dynamodb.NewImage != nil {
			newImage, err := convertFromStreamAttributeValueMap(record.Dynamodb.NewImage)
			if err != nil {
				return nil, fmt.Errorf("failed to convert new image: %w", err)
			}
			dynamoData.NewImage = newImage
		}

		if record.Dynamodb.OldImage != nil {
			oldImage, err := convertFromStreamAttributeValueMap(record.Dynamodb.OldImage)
			if err != nil {
				return nil, fmt.Errorf("failed to convert old image: %w", err)
			}
			dynamoData.OldImage = oldImage
		}

		streamRecord.DynamoDB = dynamoData
	}

	return streamRecord, nil
}

// convertToAttributeValueMap converts a map[string]interface{} to DynamoDB attribute values
func convertToAttributeValueMap(input map[string]interface{}) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue)

	for key, value := range input {
		attrValue, err := convertToAttributeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert key %s: %w", key, err)
		}
		result[key] = attrValue
	}

	return result, nil
}

// convertToAttributeValue converts a Go value to a DynamoDB attribute value
func convertToAttributeValue(value interface{}) (types.AttributeValue, error) {
	switch v := value.(type) {
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}, nil
	case string:
		return &types.AttributeValueMemberS{Value: v}, nil
	case int:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}, nil
	case int64:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}, nil
	case float64:
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'f', -1, 64)}, nil
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}, nil
	case []interface{}:
		var list []types.AttributeValue
		for _, item := range v {
			attrValue, err := convertToAttributeValue(item)
			if err != nil {
				return nil, err
			}
			list = append(list, attrValue)
		}
		return &types.AttributeValueMemberL{Value: list}, nil
	case map[string]interface{}:
		attrMap, err := convertToAttributeValueMap(v)
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: attrMap}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", value)
	}
}

// convertFromAttributeValueMap converts DynamoDB attribute values to map[string]interface{}
func convertFromAttributeValueMap(input map[string]types.AttributeValue) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for key, value := range input {
		converted, err := convertFromAttributeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert key %s: %w", key, err)
		}
		result[key] = converted
	}

	return result, nil
}

// convertFromAttributeValue converts a DynamoDB attribute value to a Go value
func convertFromAttributeValue(value types.AttributeValue) (interface{}, error) {
	switch v := value.(type) {
	case *types.AttributeValueMemberNULL:
		return nil, nil
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberN:
		// Try to parse as int first, then float
		if intVal, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			return intVal, nil
		}
		if floatVal, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return floatVal, nil
		}
		return v.Value, nil // Return as string if parsing fails
	case *types.AttributeValueMemberBOOL:
		return v.Value, nil
	case *types.AttributeValueMemberL:
		var list []interface{}
		for _, item := range v.Value {
			converted, err := convertFromAttributeValue(item)
			if err != nil {
				return nil, err
			}
			list = append(list, converted)
		}
		return list, nil
	case *types.AttributeValueMemberM:
		return convertFromAttributeValueMap(v.Value)
	case *types.AttributeValueMemberSS:
		// String set
		result := make([]interface{}, len(v.Value))
		for i, s := range v.Value {
			result[i] = s
		}
		return result, nil
	case *types.AttributeValueMemberNS:
		// Number set
		result := make([]interface{}, len(v.Value))
		for i, n := range v.Value {
			if intVal, err := strconv.ParseInt(n, 10, 64); err == nil {
				result[i] = intVal
			} else if floatVal, err := strconv.ParseFloat(n, 64); err == nil {
				result[i] = floatVal
			} else {
				result[i] = n
			}
		}
		return result, nil
	case *types.AttributeValueMemberBS:
		// Binary set - convert to base64 strings
		result := make([]interface{}, len(v.Value))
		for i, b := range v.Value {
			result[i] = string(b)
		}
		return result, nil
	case *types.AttributeValueMemberB:
		// Binary - convert to string
		return string(v.Value), nil
	default:
		return nil, fmt.Errorf("unsupported attribute value type: %T", value)
	}
}

// convertFromStreamAttributeValueMap converts DynamoDB Streams attribute values to map[string]interface{}
func convertFromStreamAttributeValueMap(input map[string]streamtypes.AttributeValue) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for key, value := range input {
		converted, err := convertFromStreamAttributeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert key %s: %w", key, err)
		}
		result[key] = converted
	}

	return result, nil
}

// convertFromStreamAttributeValue converts a DynamoDB Streams attribute value to a Go value
func convertFromStreamAttributeValue(value streamtypes.AttributeValue) (interface{}, error) {
	switch v := value.(type) {
	case *streamtypes.AttributeValueMemberNULL:
		return nil, nil
	case *streamtypes.AttributeValueMemberS:
		return v.Value, nil
	case *streamtypes.AttributeValueMemberN:
		// Try to parse as int first, then float
		if intVal, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			return intVal, nil
		}
		if floatVal, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return floatVal, nil
		}
		return v.Value, nil // Return as string if parsing fails
	case *streamtypes.AttributeValueMemberBOOL:
		return v.Value, nil
	case *streamtypes.AttributeValueMemberL:
		var list []interface{}
		for _, item := range v.Value {
			converted, err := convertFromStreamAttributeValue(item)
			if err != nil {
				return nil, err
			}
			list = append(list, converted)
		}
		return list, nil
	case *streamtypes.AttributeValueMemberM:
		return convertFromStreamAttributeValueMap(v.Value)
	case *streamtypes.AttributeValueMemberSS:
		// String set
		result := make([]interface{}, len(v.Value))
		for i, s := range v.Value {
			result[i] = s
		}
		return result, nil
	case *streamtypes.AttributeValueMemberNS:
		// Number set
		result := make([]interface{}, len(v.Value))
		for i, n := range v.Value {
			if intVal, err := strconv.ParseInt(n, 10, 64); err == nil {
				result[i] = intVal
			} else if floatVal, err := strconv.ParseFloat(n, 64); err == nil {
				result[i] = floatVal
			} else {
				result[i] = n
			}
		}
		return result, nil
	case *streamtypes.AttributeValueMemberBS:
		// Binary set - convert to base64 strings
		result := make([]interface{}, len(v.Value))
		for i, b := range v.Value {
			result[i] = string(b)
		}
		return result, nil
	case *streamtypes.AttributeValueMemberB:
		// Binary - convert to string
		return string(v.Value), nil
	default:
		return nil, fmt.Errorf("unsupported stream attribute value type: %T", value)
	}
}
