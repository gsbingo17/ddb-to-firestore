# DynamoDB to Firestore Migration Tool

A high-performance, production-ready tool for migrating data from Amazon DynamoDB to Google Cloud Firestore, written in Go. This tool supports both one-time migration and live replication using DynamoDB Streams.

## Features

- **Two Migration Modes**:
  - **Migrate**: One-time migration of existing data
  - **Live**: Continuous replication using DynamoDB Streams with initial migration

- **High Performance**:
  - Parallel DynamoDB scanning with configurable segments
  - Concurrent Firestore writers with batch operations
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
  - Application Default Credentials (ADC) support

- **Data Transformation**:
  - Configurable field naming conventions (preserve, snake_case, camelCase)
  - **Composite primary key support** (partition key + optional sort key)
  - **Auto-generated Firestore document IDs**
  - Custom field transformations (datetime, decimal128, array, object)
  - Automatic data type conversion

- **Production Ready**:
  - Structured logging with configurable levels
  - Health checks and status monitoring
  - Comprehensive error handling and recovery
  - Native Firestore SDK integration

## Architecture

The tool uses Google Cloud Firestore Native SDK for optimal performance and reliability:

- Direct integration with Firestore Native API
- Efficient batch operations (max 500 documents per batch)
- Support for Application Default Credentials (ADC)
- Query-based document lookup for live replication
- Indexed key fields for efficient queries

The replication mechanism mirrors DynamoDB Streams:
- Uses `TRIM_HORIZON` to read from the beginning of streams
- Filters records by `ApproximateCreationDateTime` 
- Handles INSERT, MODIFY, and REMOVE operations
- Implements retry logic and error handling patterns

## Prerequisites

- Go 1.21 or later
- Access to DynamoDB (AWS account or DynamoDB Local)
- Google Cloud Firestore project
- GCP credentials (service account JSON or Application Default Credentials)
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
    "projectId": "your-gcp-project-id",
    "credentialsFile": "/path/to/service-account-key.json"
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
        "keyFields": {
          "partitionKey": "id",
          "sortKey": ""
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
- `batchSize`: Number of items to process in each batch (default: 128, max: 500 for Firestore)
- `checkpointFrequency`: Save checkpoint every N records (default: 100)
- `maxRetries`: Maximum retry attempts for failed operations (default: 3)
- `retryDelayMs`: Base delay between retries in milliseconds (default: 1000)

#### Parallelism Settings
- `dynamodbReaders`: Number of parallel DynamoDB readers (default: 4)
- `firestoreWriters`: Number of parallel Firestore writers (default: 4)
- `segmentCount`: DynamoDB parallel scan segments (default: 8)

#### Firestore Configuration
- `projectId`: GCP project ID (required)
- `credentialsFile`: Path to service account JSON file (optional - uses ADC if not provided)

**Note**: The `credentialsFile` is optional. If not provided, the tool will use Application Default Credentials (ADC), which can be configured via:
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- GCloud CLI authentication (`gcloud auth application-default login`)
- Service account attached to compute resources (GCE, Cloud Run, etc.)

#### Database Pairs
Each pair defines:
- `source`: DynamoDB table configuration
- `target`: Firestore database and collection
- `mapping`: Data transformation rules

#### Mapping Options
- `collectionNaming`: Collection naming convention (preserve, snake_case, camelCase)
- `fieldNaming`: Field naming convention (preserve, snake_case, camelCase)
- `indexCreation`: Auto-create indexes based on DynamoDB keys
- `keyFields`: Composite key configuration
  - `partitionKey`: DynamoDB partition key field name (required)
  - `sortKey`: DynamoDB sort key field name (optional, leave empty if not used)
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

## Authentication

### Option 1: Service Account Key File
```json
{
  "firestore": {
    "projectId": "your-project-id",
    "credentialsFile": "/path/to/service-account-key.json"
  }
}
```

### Option 2: Application Default Credentials (ADC)
```json
{
  "firestore": {
    "projectId": "your-project-id"
  }
}
```

Set up ADC using one of these methods:
```bash
# Method 1: Environment variable
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Method 2: gcloud CLI
gcloud auth application-default login

# Method 3: Compute resource service account (automatic on GCE/Cloud Run)
```

### Environment Variables

Use environment variables for configuration:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-west-2
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

| DynamoDB Type | Firestore Type | Notes |
|---------------|----------------|-------|
| String | String | Direct mapping |
| Number | Number | Preserves int/float |
| Boolean | Boolean | Direct mapping |
| Map/Object | Map | Nested conversion |
| List/Array | Array | Element conversion |
| Binary | Bytes | Binary data |
| String Set | Array | Converted to array |
| Number Set | Array | Converted to array |
| Binary Set | Array | Array of bytes |
| NULL | Null | Direct mapping |

## Composite Primary Key Support

### Overview

Firestore documents use auto-generated IDs, but the tool preserves DynamoDB's composite key structure as indexed fields within each document for efficient querying and updates.

### Key Storage Strategy

**DynamoDB Table with Composite Key:**
```json
// Partition Key: "userId"
// Sort Key: "timestamp"
{
  "userId": "user123",
  "timestamp": "2024-01-15T10:30:00Z",
  "name": "John Doe",
  "email": "john@example.com"
}
```

**Firestore Document:**
```json
// Document ID: auto-generated (e.g., "abc123xyz")
{
  "__key_userId": "user123",        // Indexed partition key
  "__key_timestamp": "2024-01-15T10:30:00Z",  // Indexed sort key
  "userId": "user123",              // Original field
  "timestamp": "2024-01-15T10:30:00Z",  // Original field
  "name": "John Doe",
  "email": "john@example.com"
}
```

### Configuration

```json
{
  "mapping": {
    "keyFields": {
      "partitionKey": "userId",
      "sortKey": "timestamp"
    }
  }
}
```

### Simple Primary Key (No Sort Key)

For tables with only a partition key:

```json
{
  "mapping": {
    "keyFields": {
      "partitionKey": "id",
      "sortKey": ""
    }
  }
}
```

**DynamoDB:**
```json
{
  "id": "user123",
  "name": "John Doe"
}
```

**Firestore:**
```json
// Document ID: auto-generated
{
  "__key_id": "user123",  // Indexed partition key
  "id": "user123",        // Original field
  "name": "John Doe"
}
```

### Benefits

1. **Auto-generated IDs**: Firestore manages document IDs automatically
2. **Efficient Queries**: Indexed key fields enable fast lookups
3. **Update Support**: Live replication can find and update existing documents
4. **Preserved Structure**: Original DynamoDB fields remain unchanged

### Live Replication Updates

During live replication, the tool uses indexed key fields to locate documents:

```go
// Query for existing document using composite key
query := collection.
    Where("__key_userId", "==", "user123").
    Where("__key_timestamp", "==", "2024-01-15T10:30:00Z")
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
- Special handling for transient errors

### Stream Processing
- Handles expired shard iterators
- Continues processing other shards on failure
- Rate limiting to avoid API throttling

### Recovery
- Automatic resume from checkpoints
- Graceful shutdown handling
- Comprehensive error logging

## Batch Operations

Firestore Native SDK enforces a maximum of 500 operations per batch:

- Configure `batchSize` to 500 or less
- Tool automatically splits larger batches
- Optimal performance typically achieved with 128-256 documents per batch

## Development

### Project Structure
```
ddb-to-firestore/
├── cmd/migrate/           # CLI entry point
├── internal/
│   ├── config/           # Configuration management
│   ├── dynamodb/         # DynamoDB client
│   ├── firestore/        # Firestore client (Native SDK)
│   ├── checkpoint/       # Checkpoint management
│   ├── converter/        # Data conversion
│   ├── processor/        # Migration logic
│   └── utils/           # Utilities and logging
├── configs/             # Configuration examples
└── docs/               # Documentation
```

### Running Tests
```bash
go test ./...
```

### Building
```bash
make build
```

## Migration from MongoDB API

If you're migrating from the MongoDB-compatible API version, see [MIGRATION_TO_FIRESTORE_NATIVE.md](MIGRATION_TO_FIRESTORE_NATIVE.md) for detailed migration instructions.

## Troubleshooting

### Authentication Errors
- Verify `projectId` is correct
- Ensure service account has Firestore permissions
- Check ADC setup if not using `credentialsFile`

### Performance Issues
- Adjust `batchSize` (128-256 recommended)
- Increase `firestoreWriters` for more parallelism
- Monitor Firestore quota limits

### Stream Processing
- Verify DynamoDB Streams is enabled
- Check IAM permissions for stream access
- Review checkpoint files for resume state

## License

This project is licensed under the MIT License - see the LICENSE file for details.
