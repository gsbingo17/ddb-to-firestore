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
    "batchSize": 128,
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
    "connectionString": "mongodb://UID.LOCATION.firestore.goog:443/DATABASE_ID?loadBalanced=true&tls=true&retryWrites=false"
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
    "connectionString": "mongodb://UID.LOCATION.firestore.goog:443/DATABASE_ID?loadBalanced=true&tls=true&retryWrites=false"
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

### Checkpoint Storage
- **File**: JSON files in specified directory

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

## License

This project is licensed under the MIT License - see the LICENSE file for details.
