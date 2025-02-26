# BoltDB Migration Tool

A robust and reliable tool for migrating BoltDB databases with support for chunked processing, resumability, and verification.

## Features

- **Chunked Migration**: Processes large databases in manageable chunks to control memory usage
- **Resumable Operations**: Can resume from the last successful point if interrupted
- **Data Verification**: Verifies migrated data using FNV hash checksums
- **Progress Tracking**: Maintains detailed progress information during migration
- **Nested Bucket Support**: Handles nested bucket structures with proper path tracking
- **Sequence Number Preservation**: Maintains bucket sequence numbers during migration

## Usage

```go
import "github.com/yourusername/migrateboltdb"

// Create configuration
config := migrateboltdb.Config{
    ChunkSize:     20 * 1024 * 1024, // 20MB chunks
    StateFilePath: "migration.state", // For resumability
    SourceDB:      sourceDB,
    TargetDB:      targetDB,
    Logger:        logger,            // Optional, defaults to disabled
}

// Create migrator
migrator, err := migrateboltdb.New(config)
if err != nil {
    return err
}

// Perform migration
if err := migrator.Migrate(context.Background()); err != nil {
    return err
}

// Verify migration
if err := migrator.VerifyMigration(context.Background()); err != nil {
    return err
}
```

## How It Works

### Migration Process

1. **Initialization**:

   - Reads the state file if resuming a previous migration
   - Caches root buckets from the source database
   - Creates corresponding buckets in the target database

2. **Chunked Processing**:

   - Processes data in configurable chunk sizes
   - Tracks progress using bucket paths and last processed keys
   - Commits each chunk separately to manage memory usage

3. **Data Handling**:
   - Copies key-value pairs maintaining order
   - Preserves bucket hierarchy and sequence numbers
   - Handles nested buckets recursively

### Verification Process

1. **Structural Verification**:

   - Ensures all buckets exist in both databases
   - Verifies bucket sequence numbers match
   - Checks bucket hierarchy integrity

2. **Data Verification**:

   - Computes FNV hashes of bucket contents
   - Verifies key-value pairs match exactly
   - Ensures no extra or missing keys

3. **Chunked Verification**:
   - Processes verification in chunks like migration
   - Maintains checksums across chunk boundaries
   - Provides detailed mismatch information

## Important Notes

1. **Database Compatibility**:

   - Designed specifically for BoltDB to SQL migrations
   - Relies on BoltDB's lexicographical key ordering
   - Not suitable for other database types

2. **Resource Management**:

   - Controls memory usage through chunked processing
   - Manages transaction boundaries automatically
   - Handles large databases efficiently

3. **Error Handling**:
   - Provides detailed error information
   - Maintains data integrity during failures
   - Supports clean recovery through state files

## Requirements

- Go 1.x or higher
- BoltDB compatible source database
- Sufficient disk space for target database, can be adjusted via the chunk size

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
