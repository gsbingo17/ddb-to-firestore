package converter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ddb-to-firestore/internal/config"

	"go.uber.org/zap"
)

// DocumentConverter handles conversion from DynamoDB items to MongoDB documents
type DocumentConverter struct {
	mapping config.MappingConfig
	logger  *zap.Logger
}

// NewDocumentConverter creates a new document converter
func NewDocumentConverter(mapping config.MappingConfig, logger *zap.Logger) *DocumentConverter {
	return &DocumentConverter{
		mapping: mapping,
		logger:  logger,
	}
}

// ConvertDocument converts a DynamoDB item to a Firestore document
// Returns: (documentID, document, error)
// documentID will be empty string to let Firestore auto-generate
func (c *DocumentConverter) ConvertDocument(item map[string]interface{}) (string, map[string]interface{}, error) {
	if item == nil {
		return "", nil, fmt.Errorf("input item is nil")
	}

	result := make(map[string]interface{})

	// Handle primary key mapping - store keys as indexed fields
	if c.mapping.PrimaryKeyMapping != nil {
		// Store partition key
		if c.mapping.PrimaryKeyMapping.PartitionKey != nil {
			pkValue, pkExists := item[c.mapping.PrimaryKeyMapping.PartitionKey.SourceField]
			if pkExists {
				convertedPK, err := c.convertValue(c.mapping.PrimaryKeyMapping.PartitionKey.SourceField, pkValue)
				if err != nil {
					return "", nil, fmt.Errorf("failed to convert partition key: %w", err)
				}
				result[c.mapping.PrimaryKeyMapping.PartitionKey.TargetField] = convertedPK
			}
		}

		// Store sort key if exists
		if c.mapping.PrimaryKeyMapping.SortKey != nil {
			skValue, skExists := item[c.mapping.PrimaryKeyMapping.SortKey.SourceField]
			if skExists {
				convertedSK, err := c.convertValue(c.mapping.PrimaryKeyMapping.SortKey.SourceField, skValue)
				if err != nil {
					return "", nil, fmt.Errorf("failed to convert sort key: %w", err)
				}
				result[c.mapping.PrimaryKeyMapping.SortKey.TargetField] = convertedSK
			}
		}

		// Create composite key field if configured
		if c.mapping.PrimaryKeyMapping.CompositeKeyField != "" {
			compositeKey := c.buildCompositeKey(item)
			if compositeKey != "" {
				result[c.mapping.PrimaryKeyMapping.CompositeKeyField] = compositeKey
			}
		}
	}

	// Convert other fields (excluding source key fields to avoid duplication)
	for key, value := range item {
		// Skip source key fields if they've been mapped
		if c.mapping.PrimaryKeyMapping != nil {
			if c.mapping.PrimaryKeyMapping.PartitionKey != nil && key == c.mapping.PrimaryKeyMapping.PartitionKey.SourceField {
				continue
			}
			if c.mapping.PrimaryKeyMapping.SortKey != nil && key == c.mapping.PrimaryKeyMapping.SortKey.SourceField {
				continue
			}
		}

		// Apply field naming convention
		firestoreKey := config.ApplyNamingConvention(key, c.mapping.FieldNaming)

		// Convert value
		convertedValue, err := c.convertValue(key, value)
		if err != nil {
			c.logger.Warn("Failed to convert field",
				zap.String("field", key),
				zap.Error(err),
				zap.Any("value", value))
			continue
		}

		result[firestoreKey] = convertedValue
	}

	// Apply custom transforms
	if err := c.applyCustomTransforms(result); err != nil {
		return "", nil, fmt.Errorf("failed to apply custom transforms: %w", err)
	}

	// Return empty string for document ID (Firestore will auto-generate)
	return "", result, nil
}

// ConvertValue converts a single value from DynamoDB to MongoDB format (public method)
func (c *DocumentConverter) ConvertValue(fieldName string, value interface{}) (interface{}, error) {
	return c.convertValue(fieldName, value)
}

// convertValue converts a single value from DynamoDB to MongoDB format
func (c *DocumentConverter) convertValue(fieldName string, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case int, int32, int64:
		return v, nil
	case float32, float64:
		return v, nil
	case bool:
		return v, nil
	case []interface{}:
		// Convert array elements
		var result []interface{}
		for i, item := range v {
			converted, err := c.convertValue(fmt.Sprintf("%s[%d]", fieldName, i), item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert array item %d: %w", i, err)
			}
			result = append(result, converted)
		}
		return result, nil
	case map[string]interface{}:
		// Convert nested object
		result := make(map[string]interface{})
		for key, val := range v {
			mongoKey := config.ApplyNamingConvention(key, c.mapping.FieldNaming)
			converted, err := c.convertValue(fmt.Sprintf("%s.%s", fieldName, key), val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert nested field %s: %w", key, err)
			}
			result[mongoKey] = converted
		}
		return result, nil
	default:
		// For unknown types, try to convert to JSON string
		c.logger.Debug("Converting unknown type to JSON",
			zap.String("field", fieldName),
			zap.String("type", fmt.Sprintf("%T", value)))

		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal unknown type %T to JSON: %w", value, err)
		}
		return string(jsonBytes), nil
	}
}

// applyCustomTransforms applies custom field transformations
func (c *DocumentConverter) applyCustomTransforms(document map[string]interface{}) error {
	for _, transform := range c.mapping.CustomTransforms {
		if err := c.applyTransform(document, transform); err != nil {
			return fmt.Errorf("failed to apply transform for field %s: %w", transform.Field, err)
		}
	}
	return nil
}

// applyTransform applies a single transformation
func (c *DocumentConverter) applyTransform(document map[string]interface{}, transform config.TransformConfig) error {
	value, exists := c.getNestedValue(document, transform.Field)
	if !exists {
		// Field doesn't exist, skip transformation
		return nil
	}

	transformedValue, err := c.transformValue(value, transform.Type)
	if err != nil {
		return fmt.Errorf("transformation failed: %w", err)
	}

	return c.setNestedValue(document, transform.Field, transformedValue)
}

// transformValue transforms a value according to the specified type
func (c *DocumentConverter) transformValue(value interface{}, targetType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case "datetime":
		return c.transformToDateTime(value)
	case "decimal128":
		return c.transformToDecimal(value)
	case "array":
		return c.transformToArray(value)
	case "object":
		return c.transformToObject(value)
	default:
		return nil, fmt.Errorf("unsupported transform type: %s", targetType)
	}
}

// transformToDateTime converts a value to a time.Time
func (c *DocumentConverter) transformToDateTime(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		// Try to parse various datetime formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}

		// Try parsing as Unix timestamp string
		if timestamp, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(timestamp, 0), nil
		}

		return nil, fmt.Errorf("unable to parse datetime string: %s", v)
	case int64:
		// Unix timestamp
		return time.Unix(v, 0), nil
	case int:
		// Unix timestamp
		return time.Unix(int64(v), 0), nil
	case float64:
		// Unix timestamp with fractional seconds
		return time.Unix(int64(v), int64((v-float64(int64(v)))*1e9)), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to datetime", value)
	}
}

// transformToDecimal converts a value to a decimal representation
func (c *DocumentConverter) transformToDecimal(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		// Try to parse as number
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return nil, fmt.Errorf("cannot parse string as decimal: %s", v)
	case int, int32, int64:
		return v, nil
	case float32, float64:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to decimal", value)
	}
}

// transformToArray ensures the value is an array
func (c *DocumentConverter) transformToArray(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case string:
		// Try to parse as JSON array
		var arr []interface{}
		if err := json.Unmarshal([]byte(v), &arr); err == nil {
			return arr, nil
		}
		// If not JSON, wrap in array
		return []interface{}{v}, nil
	default:
		// Wrap single value in array
		return []interface{}{v}, nil
	}
}

// transformToObject ensures the value is an object
func (c *DocumentConverter) transformToObject(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		return v, nil
	case string:
		// Try to parse as JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(v), &obj); err == nil {
			return obj, nil
		}
		return nil, fmt.Errorf("cannot parse string as JSON object: %s", v)
	default:
		return nil, fmt.Errorf("cannot convert %T to object", value)
	}
}

// getNestedValue gets a value from a nested path (e.g., "user.profile.name")
func (c *DocumentConverter) getNestedValue(document map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	current := document

	for i, part := range parts {
		if i == len(parts)-1 {
			// Last part, return the value
			value, exists := current[part]
			return value, exists
		}

		// Navigate deeper
		next, exists := current[part]
		if !exists {
			return nil, false
		}

		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return nil, false
		}

		current = nextMap
	}

	return nil, false
}

// setNestedValue sets a value at a nested path
func (c *DocumentConverter) setNestedValue(document map[string]interface{}, path string, value interface{}) error {
	parts := strings.Split(path, ".")
	current := document

	for i, part := range parts {
		if i == len(parts)-1 {
			// Last part, set the value
			current[part] = value
			return nil
		}

		// Navigate or create nested structure
		next, exists := current[part]
		if !exists {
			// Create new nested map
			next = make(map[string]interface{})
			current[part] = next
		}

		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return fmt.Errorf("cannot set nested value: path %s conflicts with existing non-object value", path)
		}

		current = nextMap
	}

	return nil
}

// buildCompositeKey builds a composite key from partition and sort keys
func (c *DocumentConverter) buildCompositeKey(item map[string]interface{}) string {
	if c.mapping.PrimaryKeyMapping == nil {
		return ""
	}

	delimiter := c.mapping.PrimaryKeyMapping.Delimiter
	if delimiter == "" {
		delimiter = "#"
	}

	var parts []string

	// Add partition key
	if c.mapping.PrimaryKeyMapping.PartitionKey != nil {
		if val, exists := item[c.mapping.PrimaryKeyMapping.PartitionKey.SourceField]; exists {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}

	// Add sort key if exists
	if c.mapping.PrimaryKeyMapping.SortKey != nil {
		if val, exists := item[c.mapping.PrimaryKeyMapping.SortKey.SourceField]; exists {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}

	return strings.Join(parts, delimiter)
}

// ExtractKeyValues extracts partition and sort key values from a DynamoDB item
func (c *DocumentConverter) ExtractKeyValues(keys map[string]interface{}) (partitionKey, sortKey interface{}) {
	if c.mapping.PrimaryKeyMapping == nil {
		return nil, nil
	}

	// Extract partition key
	if c.mapping.PrimaryKeyMapping.PartitionKey != nil {
		partitionKey = keys[c.mapping.PrimaryKeyMapping.PartitionKey.SourceField]
	}

	// Extract sort key if exists
	if c.mapping.PrimaryKeyMapping.SortKey != nil {
		sortKey = keys[c.mapping.PrimaryKeyMapping.SortKey.SourceField]
	}

	return partitionKey, sortKey
}
