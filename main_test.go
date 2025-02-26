package migrateboltdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/stretchr/testify/require"

	"github.com/btcsuite/btclog/v2"
	_ "github.com/btcsuite/btcwallet/walletdb/bdb"
)

func TestMigration(t *testing.T) {
	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "boltdb_migration_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create source and target database paths
	sourceDBPath := filepath.Join(tempDir, "source.db")
	targetDBPath := filepath.Join(tempDir, "target.db")

	// Create and populate source database
	sourceDB, err := createTestDatabase(sourceDBPath)
	require.NoError(t, err)
	defer sourceDB.Close()

	// Cleanup the test database file
	defer os.Remove(sourceDBPath)
	defer os.Remove(targetDBPath)

	const (
		noFreelistSync = true
		timeout        = time.Minute
	)

	args := []interface{}{
		targetDBPath, noFreelistSync, timeout,
	}
	backend := kvdb.BoltBackendName

	// Create empty target database
	targetDB, err := walletdb.Create(backend, args...)
	require.NoError(t, err)
	defer targetDB.Close()

	consoleLogHandler := btclog.NewDefaultHandler(
		os.Stdout,
	)
	consoleLogger := btclog.NewSLogger(consoleLogHandler)
	consoleLogger.SetLevel(btclog.LevelDebug)

	// Configure and run migration
	cfg := Config{
		// Chunksize in bytes.
		ChunkSize:     2,
		StateFilePath: filepath.Join(tempDir, "migration.state"),
		SourceDB:      sourceDB,
		Logger:        consoleLogger,
		TargetDB:      targetDB,
	}

	migrator, err := New(cfg)
	require.NoError(t, err)

	err = migrator.Migrate(context.Background())
	require.NoError(t, err)

	// Verify migration
	err = verifyDatabases(t, sourceDB, targetDB)
	require.NoError(t, err)
}

func TestMigrationAndVerification(t *testing.T) {
	// Create temporary directory for test databases
	tmpDir, err := os.MkdirTemp("", "boltdb_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create source and target database paths
	sourceDBPath := filepath.Join(tmpDir, "source.db")
	targetDBPath := filepath.Join(tmpDir, "target.db")
	stateFilePath := filepath.Join(tmpDir, "migration.state")

	const (
		noFreelistSync = true
		timeout        = time.Minute
	)

	// Create and populate source database
	args := []interface{}{
		sourceDBPath, noFreelistSync, timeout,
	}
	backend := kvdb.BoltBackendName

	sourceDB, err := walletdb.Create(backend, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()

	// Populate source database with test data
	err = sourceDB.Update(func(tx walletdb.ReadWriteTx) error {
		// Create root bucket
		bucket, err := tx.CreateTopLevelBucket([]byte("test"))
		if err != nil {
			return err
		}

		// Create nested buckets and add data
		for i := 0; i < 3; i++ {
			nestedBucket, err := bucket.CreateBucket([]byte(fmt.Sprintf("nested%d", i)))
			if err != nil {
				return err
			}

			// Add some key-value pairs
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key%d", j))
				value := []byte(fmt.Sprintf("value%d", j))
				if err := nestedBucket.Put(key, value); err != nil {
					return err
				}
			}
		}
		return nil
	}, func() {})
	if err != nil {
		t.Fatal(err)
	}

	// Create target database
	args = []interface{}{
		targetDBPath, noFreelistSync, timeout,
	}

	targetDB, err := walletdb.Create(backend, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()

	consoleLogHandler := btclog.NewDefaultHandler(
		os.Stdout,
	)
	consoleLogger := btclog.NewSLogger(consoleLogHandler)
	consoleLogger.SetLevel(btclog.LevelDebug)

	// Create migrator with small chunk size to test chunking
	migrator, err := New(Config{
		ChunkSize:     1024, // Small chunk size to force multiple chunks
		StateFilePath: stateFilePath,
		SourceDB:      sourceDB,
		TargetDB:      targetDB,
		Logger:        consoleLogger,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Perform migration
	ctx := context.Background()
	if err := migrator.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify migration
	if err := migrator.VerifyMigration(ctx); err != nil {
		t.Fatal(err)
	}

	// Test resumability by creating a new migrator and verifying again
	migrator2, err := New(Config{
		ChunkSize:     1024,
		StateFilePath: stateFilePath,
		SourceDB:      sourceDB,
		TargetDB:      targetDB,
		Logger:        consoleLogger,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator2.VerifyMigration(ctx); err != nil {
		t.Fatal(err)
	}

	// Test verification with different chunk sizes
	migrator3, err := New(Config{
		ChunkSize:     4096, // Different chunk size
		StateFilePath: stateFilePath,
		SourceDB:      sourceDB,
		TargetDB:      targetDB,
		Logger:        consoleLogger,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator3.VerifyMigration(ctx); err != nil {
		t.Fatal(err)
	}
}

func createTestDatabase(dbPath string) (walletdb.DB, error) {
	const (
		noFreelistSync = true
		timeout        = time.Minute
	)
	fmt.Println("creating test database")

	args := []interface{}{
		dbPath, noFreelistSync, timeout,
	}
	backend := kvdb.BoltBackendName
	db, err := walletdb.Create(backend, args...)
	if err != nil {
		return nil, err
	}

	// Create test data structure
	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		fmt.Println("Creating test data structure...")
		// Create root bucket "accounts"
		accounts, err := tx.CreateTopLevelBucket([]byte("accounts"))
		if err != nil {
			fmt.Print("bucket creation failed.")
		}

		// Create nested buckets and add some key-value pairs
		for i := 1; i <= 3; i++ {
			userBucket, err := accounts.CreateBucketIfNotExists([]byte("user" + strconv.Itoa(i)))
			if err != nil {
				return err
			}

			err = userBucket.Put([]byte("name"), []byte("Alice"))
			if err != nil {
				return err
			}

			err = userBucket.Put([]byte("email"), []byte("alice@example.com"))
			if err != nil {
				return err
			}

			// Create a nested bucket for transactions
			txBucket, err := userBucket.CreateBucketIfNotExists([]byte("transactions"))
			if err != nil {
				return err
			}

			err = txBucket.Put([]byte("tx1"), []byte("100 BTC"))
			if err != nil {
				return err
			}
		}

		return nil
	}, func() {})

	return db, err
}

func verifyDatabases(t *testing.T, sourceDB, targetDB walletdb.DB) error {
	return sourceDB.View(func(sourceTx walletdb.ReadTx) error {
		return targetDB.View(func(targetTx walletdb.ReadTx) error {
			// Helper function to compare buckets recursively
			var compareBuckets func(source, target walletdb.ReadBucket) error
			compareBuckets = func(source, target walletdb.ReadBucket) error {
				// Compare all key-value pairs
				return source.ForEach(func(k, v []byte) error {
					if v == nil {
						// This is a nested bucket
						sourceBucket := source.NestedReadBucket(k)
						targetBucket := target.NestedReadBucket(k)
						require.NotNil(t, targetBucket)
						return compareBuckets(sourceBucket, targetBucket)
					}

					// This is a key-value pair
					targetValue := target.Get(k)
					require.Equal(t, v, targetValue)
					return nil
				})
			}

			// Compare root buckets
			return sourceTx.ForEachBucket(func(name []byte) error {
				sourceBucket := sourceTx.ReadBucket(name)
				targetBucket := targetTx.ReadBucket(name)
				require.NotNil(t, targetBucket)
				return compareBuckets(sourceBucket, targetBucket)
			})
		}, func() {})
	}, func() {})
}
