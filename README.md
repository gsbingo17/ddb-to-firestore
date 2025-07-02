# DynamoDB to Firestore Migration Tool

A high-performance, production-ready tool for migrating data from Amazon DynamoDB to Google Cloud Firestore, written in Go. This tool supports both one-time migration and live replication using DynamoDB Streams.

## Features

- **Two Migration Modes**:
  - **Migrate**: One-time migration of existing data
  - **Live**: Continuous replication using DynamoDB Streams with initial migration

- **High Performance**:
  - Parallel DynamoDB scanning with configurable segments
  - Concurrent Firestore with MongoDB writers
  - Configurable batch sizes and parallelism per database pair

- **Robust Error Handling**:
  - Exponential backoff retry logic
  - Checkpoint system for resumable operations
  - Graceful handling of stream iterator expiration

- **Flexible Configuration**:
  - JSON-based configuration
  - Support for multiple database pairs
  - Per-pair processing overrides
  - Environment variable substitution

- **Data Transformation**:
  - Configurable field naming conventions (preserve, snake_case, camelCase)
  - **Primary key mapping** (DynamoDB → MongoDB `_id`)
  - Custom field transformations (datetime, decimal128, array, object)
  - Automatic data type conversion

- **Production Ready**:
  - Structured logging with configurable levels
  - Health checks and status monitoring
  - Comprehensive error handling and recovery

## Architecture

The tool replicates the exact same DynamoDB Streams mechanism as the original Node.js implementation:

- Uses `TRIM_HORIZON` to read from the beginning of streams
- Filters records by `ApproximateCreationDateTime` 
- Handles INSERT, MODIFY, and REMOVE operations
- Implements the same retry logic and error handling patterns

## Prerequisites

- Go 1.21 or later
- Access to DynamoDB (AWS account or DynamoDB Local)
- Google Cloud Firestore project and credentials
- DynamoDB table with Streams enabled (for live replication)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd ddb-to-firestore
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Build the binary:
   ```bash
   go build -o ddb-to-firestore ./cmd/migrate
   ```

## Configuration

Create a `config.json` file based on the example:

```json
{
  "migration": {
    "mode": "migrate",
    "batchSize": 1000,
    "checkpointFrequency": 100,
    "maxRetries": 3,
    "retryDelayMs": 1000
  },
  "parallelism": {
    "dynamodbReaders": 4,
    "firestoreWriters": 4,
    "segmentCount": 8
  },
  "firestore": {
    "connectionString": "mongodb://localhost:27017"
  },
  "databasePairs": [
    {
      "name": "users-migration",
      "source": {
        "region": "us-west-2",
        "tableName": "users-table",
        "endpoint": "http://localhost:8000"
      },
      "target": {
        "database": "production",
        "collection": "users"
      },
      "mapping": {
        "collectionNaming": "preserve",
        "fieldNaming": "preserve",
        "indexCreation": true,
        "primaryKeyMapping": {
          "sourceField": "id",
          "targetField": "_id"
        }
      }
    }
  ],
  "checkpoint": {
    "storage": "file",
    "path": "./checkpoints"
  },
  "logging": {
    "level": "info",
    "format": "json",
    "output": "stdout"
  }
}
```

### Configuration Options

#### Global Settings
- `batchSize`: Number of items to process in each batch (default: 1000)
- `checkpointFrequency`: Save checkpoint every N records (default: 100)
- `maxRetries`: Maximum retry attempts for failed operations (default: 3)
- `retryDelayMs`: Base delay between retries in milliseconds (default: 1000)

#### Parallelism Settings
- `dynamodbReaders`: Number of parallel DynamoDB readers (default: 4)
- `firestoreWriters`: Number of parallel Firestore writers (default: 4)
- `segmentCount`: DynamoDB parallel scan segments (default: 8)

#### Firestore Configuration
- `connectionString`: MongoDB-compatible connection string for Firestore

#### Database Pairs
Each pair defines:
- `source`: DynamoDB table configuration
- `target`: Firestore database and collection
- `mapping`: Data transformation rules

#### Mapping Options
- `collectionNaming`: Collection naming convention (preserve, snake_case, camelCase)
- `fieldNaming`: Field naming convention (preserve, snake_case, camelCase)
- `indexCreation`: Auto-create indexes based on DynamoDB keys
- `primaryKeyMapping`: Map DynamoDB primary key to MongoDB `_id` field
- `customTransforms`: Custom field transformations

## Usage

### One-time Migration
```bash
./ddb-to-firestore migrate --config config.json
```

### Live Replication
```bash
./ddb-to-firestore live --config config.json
```

### Resume from Checkpoint
```bash
./ddb-to-firestore migrate --config config.json --resume
```

**Note**: Live replication automatically resumes from the last checkpoint when restarted, even without the `--resume` flag. The `--resume` flag is optional for live mode and primarily used for migrate mode.

### Process Specific Pairs
```bash
./ddb-to-firestore migrate --config config.json --pairs "users-migration,orders-migration"
```

### Parallel Migration
```bash
./ddb-to-firestore migrate --config config.json --segments 16
```

### Dry Run
```bash
./ddb-to-firestore migrate --config config.json --dry-run
```

### Health Check
```bash
./ddb-to-firestore health --config config.json
```

### Status Check
```bash
./ddb-to-firestore status --config config.json
```

### Validate Configuration
```bash
./ddb-to-firestore validate --config config.json
```

## Environment Variables

Use environment variables for sensitive configuration:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
export FIRESTORE_PROJECT_ID=your-project-id
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

Reference in config:
```json
{
  "firestore": {
    "connectionString": "mongodb://localhost:27017/${FIRESTORE_PROJECT_ID}"
  }
}
```

## Checkpoint System

The tool uses a robust checkpoint system:

### Migrate Mode
- Tracks `LastEvaluatedKey` for each segment
- Supports resumable parallel scanning
- Stores progress per segment
- Requires `--resume` flag to continue from checkpoint

### Live Mode  
- **Auto-Resume**: Automatically resumes from last checkpoint when restarted
- Uses timestamps from DynamoDB Streams
- Filters records by `ApproximateCreationDateTime`
- Tracks progress per shard
- Validates stream ARN compatibility on resume

### Auto-Resume Behavior

**Live Mode Auto-Resume Logic:**
1. **Always checks for existing checkpoints** when starting live replication
2. **If checkpoint exists**: Automatically resumes from last processed timestamp
3. **If no checkpoint**: Starts new replication with initial migration
4. **Stream ARN validation**: Warns and updates if stream ARN has changed
5. **No data loss**: Ensures no stream records are missed during downtime

**Example Scenarios:**
```bash
# First run - creates new checkpoint and runs initial migration
./ddb-to-firestore live --config config.json

# Restart after crash - automatically resumes from checkpoint
./ddb-to-firestore live --config config.json  # No --resume needed!

# Force new replication (future enhancement)
./ddb-to-firestore live --config config.json --force-new
```

### Checkpoint Storage
- **File**: JSON files in specified directory
- **MongoDB**: Store checkpoints in MongoDB collection

## Data Type Mapping

| DynamoDB Type | MongoDB Type | Notes |
|---------------|--------------|-------|
| String | String | Direct mapping |
| Number | Number | Preserves int/float |
| Boolean | Boolean | Direct mapping |
| Map/Object | Object | Nested conversion |
| List/Array | Array | Element conversion |
| Binary | String | Base64 encoded |
| String Set | Array | Converted to array |
| Number Set | Array | Converted to array |
| Binary Set | Array | Base64 encoded array |

## Primary Key Mapping

### **Critical Feature for Update/Delete Operations**

The primary key mapping feature ensures that DynamoDB's primary key is correctly mapped to MongoDB's `_id` field, which is essential for proper update and delete operations.

### **Problem Without Primary Key Mapping**

**❌ Without primary key mapping:**
```json
// DynamoDB Item
{
  "id": "user123",
  "name": "John Doe",
  "email": "john@example.com"
}

// MongoDB Document (WRONG)
{
  "id": "user123",           // Should be _id
  "name": "John Doe",
  "email": "john@example.com"
}

// Result: Updates create new documents instead of updating existing ones
```

**✅ With primary key mapping:**
```json
// DynamoDB Item
{
  "id": "user123",
  "name": "John Doe", 
  "email": "john@example.com"
}

// MongoDB Document (CORRECT)
{
  "_id": "user123",          // Mapped from id
  "name": "John Doe",
  "email": "john@example.com"
}

// Result: Updates correctly modify existing documents
```

### **Configuration**

Add primary key mapping to your database pair configuration:

```json
{
  "databasePairs": [
    {
      "name": "users-migration",
      "mapping": {
        "collectionNaming": "preserve",
        "fieldNaming": "preserve",
        "indexCreation": true,
        "primaryKeyMapping": {
          "sourceField": "id",
          "targetField": "_id"
        }
      }
    }
  ]
}
```

### **Configuration Options**

- **`sourceField`**: The DynamoDB primary key field name (e.g., "id", "userId", "pk")
- **`targetField`**: The MongoDB target field name (typically "_id")

### **Common Primary Key Mappings**

```json
// Simple ID mapping
"primaryKeyMapping": {
  "sourceField": "id",
  "targetField": "_id"
}

// User ID mapping
"primaryKeyMapping": {
  "sourceField": "userId",
  "targetField": "_id"
}

// Custom partition key
"primaryKeyMapping": {
  "sourceField": "pk",
  "targetField": "_id"
}
```

### **Operation Examples**

#### **INSERT Operation:**
```
DynamoDB: INSERT {"id": "user123", "name": "John"}
MongoDB: db.collection.insertOne({"_id": "user123", "name": "John"})
```

#### **UPDATE Operation:**
```
DynamoDB: UPDATE SET name = "John Smith" WHERE id = "user123"
MongoDB: db.collection.replaceOne({"_id": "user123"}, {"_id": "user123", "name": "John Smith", ...})
```

#### **DELETE Operation:**
```
DynamoDB: DELETE WHERE id = "user123"
MongoDB: db.collection.deleteOne({"_id": "user123"})
```

### **Benefits**

- ✅ **Correct Update Behavior**: Updates modify existing documents instead of creating duplicates
- ✅ **Accurate Delete Operations**: Deletes target the correct documents
- ✅ **Data Consistency**: Maintains proper data synchronization
- ✅ **Configurable**: Supports different DynamoDB key naming conventions

## Enhanced Live Mode Logic

### **Two-Stage Live Replication**

Live mode now uses an intelligent two-stage approach:

#### **Stage 1: Initial Migration**
- **Purpose**: Migrate existing data without checkpointing complexity
- **Method**: Uses `MigrateSimple()` - streamlined migration without segment checkpoints
- **Behavior**: Fast, simple migration optimized for getting to streaming quickly

#### **Stage 2: Stream Processing**
- **Purpose**: Process real-time changes from DynamoDB Streams
- **Method**: Uses timestamp-based checkpointing for stream records
- **Behavior**: Continuous processing with proper checkpoint management

### **Initial Migration Completion Tracking**

The system now tracks whether the initial migration has completed:

```json
// Checkpoint during initial migration
{
  "tableName": "users-table",
  "mode": "live",
  "timestamp": "2025-07-02T20:22:00.000Z",
  "initialMigrationComplete": false,
  "startTime": "2025-07-02T20:22:00.000Z"
}

// Checkpoint after initial migration completes
{
  "tableName": "users-table", 
  "mode": "live",
  "timestamp": "2025-07-02T20:22:00.000Z",
  "initialMigrationComplete": true,
  "startTime": "2025-07-02T20:22:00.000Z"
}
```

### **Smart Resume Logic**

The enhanced resume logic handles different failure scenarios:

#### **Scenario 1: Fresh Start**
```bash
./ddb-to-firestore live --config config.json
# Creates checkpoint with initialMigrationComplete: false
# Runs initial migration → Sets initialMigrationComplete: true
# Starts stream processing
```

#### **Scenario 2: Migration Failed, System Restarts**
```bash
# Previous run failed during migration
./ddb-to-firestore live --config config.json
# Finds checkpoint with initialMigrationComplete: false
# Runs migration again → Sets initialMigrationComplete: true
# Starts stream processing
```

#### **Scenario 3: Stream Processing Failed, System Restarts**
```bash
# Previous run completed migration but stream processing failed
./ddb-to-firestore live --config config.json
# Finds checkpoint with initialMigrationComplete: true
# Skips migration
# Resumes stream processing from last timestamp
```

### **Benefits of Enhanced Live Mode**

- ✅ **Reliable Resume**: Correctly handles migration vs. streaming failures
- ✅ **No Duplicate Work**: Skips migration when already completed
- ✅ **Fast Recovery**: Quick restart for stream processing failures
- ✅ **Clear State Tracking**: Explicit tracking of migration completion

## Custom Transformations

Transform specific fields during migration:

```json
{
  "customTransforms": [
    {
      "field": "createdAt",
      "type": "datetime"
    },
    {
      "field": "price",
      "type": "decimal128"
    },
    {
      "field": "tags",
      "type": "array"
    }
  ]
}
```

## Monitoring and Logging

### Structured Logging
```json
{
  "logging": {
    "level": "info",
    "format": "json",
    "output": "both",
    "file": "./logs/migration.log",
    "perPairLogs": true
  }
}
```

### Log Levels
- `debug`: Detailed debugging information
- `info`: General information about progress
- `warn`: Warning messages for non-critical issues
- `error`: Error messages for failures

### Progress Monitoring
The tool provides detailed progress information:
- Records processed per second
- Batch processing status
- Checkpoint timestamps
- Error details and retry attempts

## Error Handling

### Retry Logic
- Exponential backoff with configurable delays
- Per-operation retry limits
- Special handling for duplicate key errors

### Stream Processing
- Handles expired shard iterators
- Continues processing other shards on failure
- Rate limiting to avoid API throttling

### Recovery
- Automatic resume from checkpoints
- Graceful shutdown handling
- Comprehensive error logging

## Performance Tuning

### For Large Tables
```json
{
  "parallelism": {
    "dynamodbReaders": 8,
    "firestoreWriters": 12,
    "segmentCount": 20
  },
  "migration": {
    "batchSize": 2000
  }
}
```

### For High Throughput
```json
{
  "parallelism": {
    "dynamodbReaders": 8,
    "firestoreWriters": 16,
    "segmentCount": 12
  },
  "migration": {
    "batchSize": 500,
    "retryDelayMs": 500
  }
}
```

## Stream Processing Architecture

### **Improved Shard Processing (v2.0)**

The tool now uses an optimized approach for DynamoDB Streams processing:

#### **Unlimited Shard Concurrency**
- **All shards process simultaneously**: No artificial limits on concurrent shard processing
- **Natural rate limiting**: Relies on DynamoDB Streams' built-in rate limits (1000 records/sec per shard)
- **Better resource utilization**: Eliminates bottlenecks from semaphore-based concurrency control

#### **Architecture Comparison**

| Aspect | Migration Mode | Stream Replication Mode |
|--------|----------------|------------------------|
| **Parallelism Control** | `dynamodbReaders` controls table scan segments | **No limits** - all shards process concurrently |
| **Data Source** | DynamoDB table scan | DynamoDB Streams shards |
| **Concurrency Unit** | Table segments (limited) | Stream shards (unlimited) |
| **Processing Pattern** | Parallel segments → batch writes | **All shards parallel** → sequential records → batch writes |

#### **Stream Processing Flow**
```
DynamoDB Stream
├── Shard 1 ──┐
├── Shard 2 ──┤
├── Shard 3 ──┼── All Shards Process Concurrently ──┐
├── Shard 4 ──┤                                      ├── Batch Writer Pool
└── Shard N ──┘                                      └── (firestoreWriters=4)
```

#### **Key Benefits**
1. **Maximum Throughput**: All available shards process data simultaneously
2. **Simplified Configuration**: No need to tune shard concurrency parameters
3. **Auto-Scaling**: Automatically adapts to the number of active shards
4. **Order Preservation**: Records within each shard maintain chronological order

#### **Performance Characteristics**
- **Shard Independence**: Each shard operates at its own optimal pace
- **Natural Backpressure**: DynamoDB Streams provides built-in rate limiting
- **Memory Efficiency**: Each shard uses minimal memory footprint
- **Error Isolation**: Failed shards don't impact other shard processing

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase `wtimeout` in write concern
   - Check network connectivity
   - Verify credentials

2. **Memory Usage**
   - Reduce `batchSize`
   - Lower `parallelism` settings
   - Monitor system resources

3. **Stream Processing Errors**
   - Verify DynamoDB Streams is enabled
   - Check IAM permissions
   - Monitor shard iterator expiration

### Debug Mode
```bash
./ddb-to-firestore migrate --config config.json --debug
```

## Development

### Project Structure
```
ddb-to-mongodb/
├── cmd/migrate/           # CLI entry point
├── internal/
│   ├── config/           # Configuration management
│   ├── dynamodb/         # DynamoDB client
│   ├── mongodb/          # MongoDB client  
│   ├── checkpoint/       # Checkpoint management
│   ├── converter/        # Data conversion
│   ├── processor/        # Migration logic
│   └── utils/           # Utilities and logging
├── configs/             # Configuration examples
└── docs/               # Documentation
```

### Building
```bash
# Build for current platform
go build -o ddb-to-firestore ./cmd/migrate

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o ddb-to-firestore-linux ./cmd/migrate

# Build for Windows  
GOOS=windows GOARCH=amd64 go build -o ddb-to-firestore.exe ./cmd/migrate
```

### Testing
```bash
# Run tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run integration tests
go test -tags=integration ./...
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the configuration examples
