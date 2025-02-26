package migrateboltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"strings"

	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
)

// ErrChunkSizeExceeded is returned when the chunk size limit is reached during
// migration, indicating that the migration should continue with a new transaction.
// It should close the reading and write transaction and continue where it stopped.
var ErrChunkSizeExceeded = fmt.Errorf("chunk size exceeded")

// RESTRICT Migration only to BBOLT => SQL because we are leveraging the fact
// that bolt db loops through keys in lexicographical order which cannot be
// guaranteed for other databases.

// Config holds the configuration for the migrator.
type Config struct {
	// ChunkSize is the number of items (key-value pairs or buckets) to
	// process in a single transaction.
	ChunkSize uint64

	// StateFilePath is the path to the file where migration state is
	// stored. If empty, state won't be persisted.
	StateFilePath string

	// Logger is the logger to use for logging.
	Logger btclog.Logger

	// SourceDB is the source database to migrate from.
	SourceDB walletdb.DB

	// TargetDB is the target database to migrate to.
	TargetDB walletdb.DB
}

// State tracks the migration progress for resumability.
type State struct {
	// Data migration phase
	CurrentBucketPath string `json:"current_bucket_path"` // Currently processing bucket
	LastProcessedKey  []byte `json:"last_processed_key"`  // Last key processed in current bucket
	ProcessedKeys     int64  `json:"processed_keys"`      // Number of keys processed
}

// BucketChecksum holds checksum information for a bucket
type BucketChecksum struct {
	IncludedPaths    []string `json:"paths"`              // All included paths in this checksum
	Hash             uint64   `json:"hash"`               // FNV hash of all key-value pairs which were processed in this chunk (including the sequence number of the buckets)
	LastProcessedKey []byte   `json:"last_processed_key"` // Last key processed of the last included bucket
}

// VerifyResult holds the result of a verification
type VerifyResult struct {
	Path        string // Bucket path
	SourceHash  uint64 // Source bucket hash
	TargetHash  uint64 // Target bucket hash
	IsMatch     bool   // Whether hashes match
	ErrorDetail string // Error details if any
}

// Migrator handles the chunked migration of a bolt database.
type Migrator struct {
	cfg                  Config
	state                *State
	keyValueCount        uint64
	overallKeyValueCount uint64
	currentChunkSize     uint64
	// When resuming a migration, we need to skip processing keys until we
	// find the bucket where we left off. This flag controls that skipping
	// behavior during the recursive bucket traversal.
	skipKeyProcessing bool
	// Hash state for verification
	sourceHash hash.Hash64
	targetHash hash.Hash64
}

// New creates a new Migrator with the given configuration.
func New(cfg Config) (*Migrator, error) {
	if cfg.ChunkSize == 0 {
		// Default to 10MB chunk size (leaving room for overhead)
		// 20MB = 20 * 1024 * 1024 bytes
		cfg.ChunkSize = 20 * 1024 * 1024
	}

	if cfg.Logger == nil {
		cfg.Logger = btclog.Disabled
	}

	return &Migrator{
		cfg:   cfg,
		state: &State{},
	}, nil
}

// isBucketMigrated checks if a bucket would have already been processed
// in our depth-first traversal based on the current path we're processing.
//
// NOTE: This is a BOLT-specific optimization and will not work for other
// databases.
func (m *Migrator) isBucketMigrated(pathToCheck string, currentPath string) bool {
	// Split both paths into components
	checkParts := strings.Split(pathToCheck, "/")
	currentParts := strings.Split(currentPath, "/")

	// Find the first differing component
	minLen := len(checkParts)
	if len(currentParts) < minLen {
		minLen = len(currentParts)
	}

	for i := 0; i < minLen; i++ {
		if checkParts[i] != currentParts[i] {
			// If we find a component that differs and it's lexicographically
			// before our current path, it must have been processed already
			return checkParts[i] < currentParts[i]
		}
	}

	return false
}

// processChunk processes a single chunk of the migration using the provided transactions,
// returning ErrChunkSizeExceeded if the chunk size limit is reached.
func (m *Migrator) processChunk(
	ctx context.Context,
	rootBuckets [][]byte,
	sourceTx, targetTx walletdb.ReadWriteTx,
) error {
	for _, rootBucket := range rootBuckets {

		sourceRootBucket := sourceTx.ReadWriteBucket(rootBucket)

		// We create the root bucket if it does not exist.
		targetRootBucket, err := targetTx.CreateTopLevelBucket(
			rootBucket,
		)
		if err != nil {
			return fmt.Errorf("failed to create target root "+
				"bucket: %w", err)
		}

		// Start with the root bucket name as the initial path
		initialPath := string(rootBucket)
		err = m.migrateBucket(
			ctx,
			sourceRootBucket,
			initialPath,
			targetRootBucket,
		)
		if err == ErrChunkSizeExceeded {
			// Caller will handle transaction commit/rollback
			return ErrChunkSizeExceeded
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Migrate performs the migration of the source database to the target database.
func (m *Migrator) Migrate(ctx context.Context) error {
	fmt.Println("migrating database ...")

	m.resetState()

	// Read state file if path is provided
	if m.cfg.StateFilePath != "" {
		state, err := readMigrationState(m.cfg.StateFilePath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to read migration state: %w", err)
		}
		if state != nil && state.CurrentBucketPath != "" {
			m.state = state
			// We have a previous migration state, so we can skip
			// all previously migrated keys.
			m.skipKeyProcessing = true
		}
	}

	// First we need to get all the buckets from the source database.
	sourceTx, err := m.cfg.SourceDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin source tx: %w", err)
	}
	defer sourceTx.Rollback()

	// We first cache the root buckets
	var rootBuckets [][]byte
	sourceTx.ForEachBucket(func(k []byte) error {
		m.cfg.Logger.Infof("Caching root-bucket: %s",
			hex.EncodeToString(k))

		rootBuckets = append(rootBuckets, k)

		return nil
	})

	// Open also a target transaction for the migration process.
	targetTx, err := m.cfg.TargetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	// Process chunks until complete.
	for {
		m.cfg.Logger.Infof("Processing chunk")
		err = m.processChunk(ctx, rootBuckets, sourceTx, targetTx)
		if err == nil {
			// Migration complete, commit final transactions.
			sourceTx.Rollback()
			if err := targetTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit final "+
					"target tx: %w", err)
			}

			if m.skipKeyProcessing {
				m.cfg.Logger.Warn("No keys were migrated, " +
					"the migration state path was not found")
			}

			m.cfg.Logger.Infof("Migration completed, total keys "+
				"processed: %d", m.overallKeyValueCount)

			// We reset the state because we are done with the migration.
			m.state = &State{}

			// Persist our progress to disk
			if err := m.saveMigrationState(); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			return nil
		}
		if err == ErrChunkSizeExceeded {
			// Commit target and rollback source for this chunk
			sourceTx.Rollback()
			if err := targetTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit target "+
					"tx: %w", err)
			}

			// Persist our progress to disk
			if err := m.saveMigrationState(); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			m.cfg.Logger.Infof("Migration progress saved, " +
				"resuming with next chunk")

			// Reset for next chunk
			m.currentChunkSize = 0
			m.keyValueCount = 0

			// We are resetting transaction so we set the flag to
			// true so we skip all previously migrated keys.
			m.skipKeyProcessing = true

			// We need to start a new transaction for the next chunk.
			sourceTx, err = m.cfg.SourceDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin source "+
					"tx: %w", err)
			}
			targetTx, err = m.cfg.TargetDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin target "+
					"tx: %w", err)
			}

			continue
		}

		return err
	}
}

// migrateBucket migrates a bucket from the source database to the target
// database.
func (m *Migrator) migrateBucket(ctx context.Context,
	sourceB walletdb.ReadWriteBucket, path string,
	targetB walletdb.ReadWriteBucket) error {

	// Skip fully processed buckets
	if m.state.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.state.CurrentBucketPath) {
		return nil
	}

	// Copying the sequence number over as well.
	if err := targetB.SetSequence(sourceB.Sequence()); err != nil {
		return fmt.Errorf("error copying sequence number")
	}

	cursor := sourceB.ReadCursor()

	// Start from the last processed key if resuming in this bucket
	var k, v []byte
	resumingInThisBucket := path == m.state.CurrentBucketPath
	if resumingInThisBucket {
		// We found the bucket where we left off, so we from now on
		// start processing the keys.
		m.cfg.Logger.Infof("Resuming migration in bucket: %s", path)
		m.skipKeyProcessing = false
	}
	if resumingInThisBucket && len(m.state.LastProcessedKey) > 0 {
		k, v = cursor.Seek(m.state.LastProcessedKey)
		if k == nil {
			return fmt.Errorf("failed to find last processed "+
				"key %x in bucket %s - database may be "+
				"corrupted", m.state.LastProcessedKey, path)
		}
		if bytes.Equal(k, m.state.LastProcessedKey) {
			k, v = cursor.Next() // Skip the last processed key
			if k == nil {
				return nil // No more keys to process
			}
		}
	} else {
		k, v = cursor.First()
	}

	// We process through the bucket.
	for ; k != nil; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle nested bucket
		if v == nil {
			sourceNestedBucket := sourceB.NestedReadWriteBucket(k)
			if sourceNestedBucket == nil {
				return fmt.Errorf("source nested bucket not "+
					"found for key %x", k)
			}
			targetNestedBucket := targetB.NestedReadWriteBucket(k)

			if targetNestedBucket == nil {
				var err error
				targetNestedBucket, err = targetB.CreateBucket(k)
				if err != nil {
					return fmt.Errorf("failed to create "+
						"bucket %x: %w", k, err)
				}
			}

			newPath := path
			if newPath == "" {
				newPath = string(k)
			} else {
				newPath += "/" + string(k)
			}

			// Recursively migrate the nested bucket
			if err := m.migrateBucket(
				ctx,
				sourceNestedBucket,
				newPath,
				targetNestedBucket,
			); err != nil {
				if err == ErrChunkSizeExceeded {
					// Propagate chunk size exceeded up the
					// stack.
					return err
				}
				return fmt.Errorf("failed to migrate bucket "+
					"%s: %w", newPath, err)
			}
			continue
		}

		if m.skipKeyProcessing {
			continue
		}

		m.cfg.Logger.Debugf("migrating key: %s, value: %s",
			loggableKeyName(k), loggableKeyName(v))

		// Copy key-value pair to target
		if err := targetB.Put(k, v); err != nil {
			return fmt.Errorf("failed to migrate key %x: %w", k,
				err)
		}

		m.state.LastProcessedKey = k
		m.state.ProcessedKeys++
		m.keyValueCount++
		m.overallKeyValueCount++

		// If chunk size is reached, commit and signal pause
		entrySize := uint64(len(k) + len(v))
		m.currentChunkSize += entrySize

		if m.currentChunkSize >= m.cfg.ChunkSize {
			m.state.CurrentBucketPath = path
			m.state.LastProcessedKey = k

			m.cfg.Logger.Infof("Chunk size exceeded at "+
				"bucket %s, last processed key: %x", path, k)

			return ErrChunkSizeExceeded
		}
	}

	return nil
}

// readMigrationState reads the migration state from the given file path.
func readMigrationState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// saveMigrationState writes the current migration state to disk.
func (m *Migrator) saveMigrationState() error {
	if m.cfg.StateFilePath == "" {
		return nil
	}

	data, err := json.Marshal(m.state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(m.cfg.StateFilePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// resetState resets the migration state.
func (m *Migrator) resetState() {
	m.state = &State{}
	m.overallKeyValueCount = 0
	m.keyValueCount = 0
	m.currentChunkSize = 0
	m.sourceHash = fnv.New64a()
	m.targetHash = fnv.New64a()
}

// VerifyMigration verifies that the source and target databases match
func (m *Migrator) VerifyMigration(ctx context.Context) error {
	m.cfg.Logger.Infof("Verifying migration ...")
	m.resetState()

	fmt.Println("Global key value count: ", m.overallKeyValueCount)

	// Read state file if path is provided
	if m.cfg.StateFilePath != "" {
		state, err := readMigrationState(m.cfg.StateFilePath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to read verification state: %w", err)
		}
		if state != nil && state.CurrentBucketPath != "" {
			m.state = state
			m.skipKeyProcessing = true
		}
	}

	// First we need to get all the buckets from the source database.
	sourceTx, err := m.cfg.SourceDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin source tx: %w", err)
	}
	defer sourceTx.Rollback()

	// Cache the root buckets
	var rootBuckets [][]byte
	sourceTx.ForEachBucket(func(k []byte) error {
		m.cfg.Logger.Infof("Caching root-bucket: %s",
			hex.EncodeToString(k))
		rootBuckets = append(rootBuckets, k)
		return nil
	})

	targetTx, err := m.cfg.TargetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	// Process chunks until complete
	for {
		// Reset hashes for each new chunk
		m.sourceHash = fnv.New64a()
		m.targetHash = fnv.New64a()

		err = m.verifyChunk(ctx, rootBuckets, sourceTx, targetTx)
		if err == nil {
			// Verification complete
			sourceTx.Rollback()
			targetTx.Rollback()

			fmt.Printf("\nVerification Complete:\n")
			fmt.Printf("Total Keys Verified: %d\n", m.overallKeyValueCount)
			fmt.Printf("✅ All buckets verified successfully!\n")

			// We need to reset the state because we are done with the
			// verification.
			m.state = &State{}

			// Persist our progress to disk
			if err := m.saveMigrationState(); err != nil {
				return fmt.Errorf("failed to save verification "+
					"state: %w", err)
			}

			return nil
		}
		if err == ErrChunkSizeExceeded {
			// Clean up transactions and continue with next chunk
			sourceTx.Rollback()
			targetTx.Rollback()

			// Print progress
			fmt.Printf("\nVerification Progress:\n")
			fmt.Printf("- Current Bucket: %s\n", m.state.CurrentBucketPath)
			fmt.Printf("- Current Chunk Size: %d bytes\n", m.currentChunkSize)
			fmt.Printf("- Overall Keys Verified: %d\n", m.overallKeyValueCount)

			// Reset for next chunk
			m.currentChunkSize = 0
			m.keyValueCount = 0
			m.skipKeyProcessing = true

			// Start new transactions
			sourceTx, err = m.cfg.SourceDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin source tx: %w", err)
			}
			targetTx, err = m.cfg.TargetDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin target tx: %w", err)
			}

			continue
		}

		return err
	}
}

// verifyChunk verifies a chunk of the migration, comparing source and target
func (m *Migrator) verifyChunk(
	ctx context.Context,
	rootBuckets [][]byte,
	sourceTx, targetTx walletdb.ReadWriteTx,
) error {
	for _, rootBucket := range rootBuckets {
		sourceRoot := sourceTx.ReadWriteBucket(rootBucket)
		targetRoot := targetTx.ReadWriteBucket(rootBucket)

		// Start with root bucket name as path
		initialPath := string(rootBucket)
		err := m.verifyBucket(ctx, sourceRoot, targetRoot, initialPath)
		if err == ErrChunkSizeExceeded {
			return ErrChunkSizeExceeded
		}
		if err != nil {
			return fmt.Errorf("failed to verify bucket %s: %w",
				initialPath, err)
		}
	}

	return nil
}

// verifyBucket verifies a single bucket matches between source and target
func (m *Migrator) verifyBucket(
	ctx context.Context,
	sourceB, targetB walletdb.ReadWriteBucket,
	path string,
) error {
	// Skip already verified buckets.
	if m.state.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.state.CurrentBucketPath) {
		return nil
	}

	// First verify sequence numbers match and include them in the hash
	sourceSeq := sourceB.Sequence()
	targetSeq := targetB.Sequence()
	if sourceSeq != targetSeq {
		return fmt.Errorf("sequence numbers don't match in bucket %s: "+
			"source=%d target=%d", path, sourceSeq, targetSeq)
	}

	// Add sequence number to both hashes
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, sourceSeq)
	m.sourceHash.Write(seqBytes)
	m.targetHash.Write(seqBytes)

	sourceCursor := sourceB.ReadCursor()
	targetCursor := targetB.ReadCursor()

	var sk, sv, tk, tv []byte
	resumingInThisBucket := path == m.state.CurrentBucketPath

	// This is the bucket we left off at (resuming). We resume processing
	// keys.
	if resumingInThisBucket {
		m.skipKeyProcessing = false
		m.cfg.Logger.Infof("Resuming verification in bucket: %s", path)
	}

	if resumingInThisBucket && len(m.state.LastProcessedKey) > 0 {
		sk, sv = sourceCursor.Seek(m.state.LastProcessedKey)
		tk, tv = targetCursor.Seek(m.state.LastProcessedKey)
		if sk == nil || tk == nil {
			return fmt.Errorf("failed to find last processed "+
				"key in bucket %s", path)
		}
		if bytes.Equal(sk, m.state.LastProcessedKey) {
			sk, sv = sourceCursor.Next()
			tk, tv = targetCursor.Next()
		}
	} else {
		sk, sv = sourceCursor.First()
	}

	for sk != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Look up the corresponding key in target
		tk, tv = targetCursor.Seek(sk)
		if tk == nil {
			return fmt.Errorf("key %s not found in target bucket %s",
				string(sk), path)
		}

		// Handle nested buckets
		if sv == nil {
			if tv != nil {
				return fmt.Errorf("bucket in source is value "+
					"in target in bucket %s", path)
			}

			sourceNested := sourceB.NestedReadWriteBucket(sk)
			targetNested := targetB.NestedReadWriteBucket(sk)

			newPath := path
			if newPath == "" {
				newPath = string(sk)
			} else {
				newPath += "/" + string(sk)
			}

			// Add bucket key to both hashes
			m.sourceHash.Write(sk)
			m.targetHash.Write(tk)

			if err := m.verifyBucket(
				ctx,
				sourceNested,
				targetNested,
				newPath,
			); err != nil {
				return err
			}

			sk, sv = sourceCursor.Next()
			continue
		}

		if m.skipKeyProcessing {
			sk, sv = sourceCursor.Next()
			continue
		}

		// Add key-value pairs to both hashes
		m.sourceHash.Write(sk)
		m.sourceHash.Write(sv)
		m.targetHash.Write(tk)
		m.targetHash.Write(tv)

		// Verify values match
		if !bytes.Equal(sv, tv) {
			return fmt.Errorf("value mismatch for key %x in "+
				"bucket %s", sk, path)
		}

		m.state.LastProcessedKey = sk
		m.state.ProcessedKeys++
		m.keyValueCount++
		m.overallKeyValueCount++

		// Check if chunk size exceeded
		entrySize := uint64(len(sk) + len(sv))
		m.currentChunkSize += entrySize

		if m.currentChunkSize >= m.cfg.ChunkSize {
			// Verify hashes before starting new chunk
			sourceSum := m.sourceHash.Sum64()
			targetSum := m.targetHash.Sum64()
			if sourceSum != targetSum {
				return fmt.Errorf("checksum mismatch at chunk "+
					"boundary in bucket %s: source=%d "+
					"target=%d", path, sourceSum, targetSum)
			}

			m.state.CurrentBucketPath = path
			return ErrChunkSizeExceeded
		}

		// Move to next source key
		sk, sv = sourceCursor.Next()
	}

	return nil
}

// loggableKeyName returns a printable name of the given key.
func loggableKeyName(key []byte) string {
	strKey := string(key)
	if hasSpecialChars(strKey) {
		return hex.EncodeToString(key)
	}

	return strKey
}

// hasSpecialChars returns true if any of the characters in the given string
// cannot be printed.
func hasSpecialChars(s string) bool {
	for _, b := range s {
		if !(b >= 'a' && b <= 'z') && !(b >= 'A' && b <= 'Z') &&
			!(b >= '0' && b <= '9') && b != '-' && b != '_' {

			return true
		}
	}

	return false
}
