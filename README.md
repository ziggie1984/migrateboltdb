# BoltDB Migration Tool

A tool for safely migrating data between BoltDB databases with built-in verification. Designed to handle large databases through chunked processing with support for resuming interrupted migrations.

## Features

- Chunked migration to handle large databases efficiently
- Built-in verification of migrated data
- Automatic resumption of interrupted migrations
- Progress tracking and logging
- Configurable chunk sizes
- Transaction safety

## Usage

```go
import "github.com/yourusername/migrateboltdb"

// Configure the migration
config := migrateboltdb.Config{
    ChunkSize:        20 * 1024 * 1024, // 20MB chunks (default)
    Logger:           logger,            // Your logger implementation
    ForceNewMigration: false,           // Set to true to restart migration
    SkipVerification:  false,           // Set to true to skip verification
}

// Create a new migrator
migrator, err := migrateboltdb.New(config)
if err != nil {
    return err
}

// Perform the migration
err = migrator.Migrate(ctx, sourceDB, targetDB)
if err != nil {
    return err
}
```

## How It Works

1. **Chunked Processing**:

   - Data is migrated in configurable chunks (default 20MB)
   - Each chunk is processed in its own transaction
   - Progress is saved after each chunk

2. **Migration Process**:

   - Walks through the source database structure
   - Copies buckets, key-value pairs, and sequence numbers
   - Maintains hash checksums for verification
   - Automatically resumes from last saved position if interrupted

3. **Built-in Verification**:

   - Automatically verifies migrated data after migration
   - Compares chunk hashes between migration and verification
   - Ensures data integrity
   - Can be skipped with `SkipVerification` config option

4. **State Management**:
   - Tracks progress in a metadata bucket
   - Stores chunk hashes for verification
   - Maintains consistent chunk sizes for hash comparison

## Configuration Options

- `ChunkSize`: Size of each migration chunk (default: 20MB, max: 200MB)
- `Logger`: Custom logger for progress tracking
- `ForceNewMigration`: Force restart of migration
- `SkipVerification`: Skip verification phase

## Important Notes

- Requires consistent chunk sizes between migration and verification
- Relies on BoltDB's lexicographical ordering of keys
- Designed specifically for BoltDB to SQL migrations
- Not suitable for databases that don't guarantee key ordering

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
