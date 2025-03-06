package migrateboltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
)

// Database bucket and key constants.
const (
	// migrationMetaBucket is the top-level bucket that stores the migration
	// metadata.
	migrationMetaBucket = "migration_meta"

	// migrationStateKey is the key of the current state of the migration.
	migrationStateKey = "migration_state"

	// verificationStateKey is the key of the verification state of the
	// migration.
	verificationStateKey = "verification_state"

	// chunkHashKey is the key of the chunk hash of the source database.
	chunkHashKey = "source_chunk_hashes"
)

// Process state markers
const (
	// migrationCompleteMarker is the marker for the migration complete
	// state.
	migrationCompleteMarker = "migration_complete"

	// verificationCompleteMarker is the marker for the verification
	// complete state.
	verificationCompleteMarker = "verification_complete"
)

// Size constants.
const (
	// DefaultChunkSize is 20MB (leaving room for overhead).
	DefaultChunkSize = 20 * 1024 * 1024

	// MaxChunkSize is 100MB to prevent excessive memory usage.
	MaxChunkSize = 200 * 1024 * 1024
)

// Config holds the configuration for the migrator.
type Config struct {
	// ChunkSize is the number of items (key-value pairs or buckets) to
	// process in a single transaction.
	ChunkSize uint64

	// Logger is the logger to use for logging.
	Logger btclog.Logger

	// ForceNewMigration is a flag to force a new migration even if a
	// migration state is found.
	ForceNewMigration bool

	// SkipVerification is a flag to skip the verification of the migration.
	SkipVerification bool
}

// validateConfig ensures the configuration is valid and sets defaults
func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Validate and set defaults for chunk size.
	switch {
	case cfg.ChunkSize == 0:
		cfg.ChunkSize = DefaultChunkSize

	case cfg.ChunkSize > MaxChunkSize:
		return fmt.Errorf("chunk size too large: %d bytes, maximum "+
			"is %d bytes", cfg.ChunkSize, MaxChunkSize)
	}

	// Ensure logger is set.
	if cfg.Logger == nil {
		cfg.Logger = btclog.Disabled
	}

	return nil
}

// ChunkHash represents the verification hash of a chunk of the source database.
type ChunkHash struct {
	// Hash is the accumulated hash of the chunk data.
	Hash uint64 `json:"hash"`

	// LastKey is the last key processed in the chunk.
	LastKey []byte `json:"last_key"`

	// Path is the last processed bucket path in that chunk.
	Path string `json:"path"`
}

// Record chunk hash in target DB.
func (m *Migrator) recordChunkHash(targetTx walletdb.ReadWriteTx) error {
	metaBucket, err := m.getOrCreateMetaBucket(targetTx)
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	record := ChunkHash{
		Hash:    m.migration.hash.Sum64(),
		LastKey: m.migration.persistedState.LastProcessedKey,
		Path:    m.migration.persistedState.CurrentBucketPath,
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Create a unique key by combining bucket path and last processed key
	key := append(
		[]byte(m.migration.persistedState.CurrentBucketPath),
		m.migration.persistedState.LastProcessedKey...,
	)
	err = metaBucket.Put(key, encoded)

	return err
}

// getChunkHash tries to get the chunk hash from a specific bucket path.
func (m *Migrator) getChunkHash(targetTx walletdb.ReadTx,
	lookupKey []byte) (ChunkHash, error) {

	metaBucket := targetTx.ReadBucket([]byte(migrationMetaBucket))
	if metaBucket == nil {
		return ChunkHash{}, errNoMetaBucket
	}

	chunkHashBytes := metaBucket.Get(lookupKey)
	if chunkHashBytes == nil {
		return ChunkHash{}, errNoChunkHash
	}

	var chunkHash ChunkHash
	if err := json.Unmarshal(chunkHashBytes, &chunkHash); err != nil {
		return ChunkHash{}, fmt.Errorf("failed to unmarshal chunk hash: %w", err)
	}

	return chunkHash, nil
}

// getOrCreateMetaBucket gets or creates the migration metadata bucket.
func (m *Migrator) getOrCreateMetaBucket(tx walletdb.ReadWriteTx) (
	walletdb.ReadWriteBucket, error) {

	metaBucket := tx.ReadWriteBucket([]byte(migrationMetaBucket))
	if metaBucket != nil {
		return metaBucket, nil
	}

	return tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
}

// Migrator handles the chunked migration of bolt databases. It supports:
//   - Resumable migrations through state tracking
//   - Chunk-based processing to handle large databases
//   - Verification of migrated data
//   - Progress logging
type Migrator struct {
	// cfg is the configuration for the migrator.
	cfg Config

	// migration is the migration state.
	migration *MigrationState

	// verification is the verification state.
	verification *VerificationState
}

// New creates a new Migrator with the given configuration.
func New(cfg Config) (*Migrator, error) {
	// Validate and set defaults for the config.
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Migrator{
		cfg: cfg,
		migration: &MigrationState{
			hash:     fnv.New64a(),
			resuming: false,
		},
		verification: &VerificationState{
			hash:     fnv.New64a(),
			resuming: false,
		},
	}, nil
}

// isBucketMigrated checks if a bucket would have already been processed
// in our depth-first traversal based on the current path we're processing.
//
// NOTE: This works because we can be sure that the nested bucket paths are
// lexicographically ordered and are always walking through in the same order.
func (m *Migrator) isBucketMigrated(pathToCheck string,
	currentPath string) bool {

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
			// If we find a component that differs and it's
			// lexicographically before our current path, it
			// must have been processed already
			return checkParts[i] < currentParts[i]
		}
	}

	return false
}

// logChunkProgress logs the progress of a chunk.
func (m *Migrator) logChunkProgress(ctx context.Context, path string,
	lastKey []byte, currentChunkBytes uint64, hash uint64) {

	m.cfg.Logger.InfoS(ctx, "Chunk size exceeded, committing:",
		"current_bucket", path,
		"last_key", loggableKeyName(lastKey),
		"chunk_size_limit", m.cfg.ChunkSize,
		"chunk_size_actual", currentChunkBytes,
		"hash", hash,
	)
}

// Migrate performs the migration of the source database to the target database.
func (m *Migrator) Migrate(ctx context.Context, sourceDB,
	targetDB walletdb.DB) error {

	m.cfg.Logger.Infof("Migrating database ...")

	// First we need to get all the buckets from the source database.
	sourceTx, err := sourceDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin source tx: %w", err)
	}
	defer sourceTx.Rollback()

	// Open also a target transaction for the migration process.
	targetTx, err := targetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	err = m.migration.read(targetTx)
	switch {
	case err != nil && !errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound):

		return fmt.Errorf("failed to read migration state: %w", err)

	case m.migration.persistedState == nil:
		// State does not exist, start fresh.
		m.cfg.Logger.Error("Migration state not found, starting fresh")
		m.migration.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.migration.resuming = false

	case m.migration.persistedState.CurrentBucketPath == "":
		// State exists but empty path, start fresh.
		m.cfg.Logger.Error("Migration state found but empty path, " +
			"starting fresh")
		m.migration.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.migration.resuming = false

	case m.cfg.ForceNewMigration:
		// Force new migration, start fresh.
		m.cfg.Logger.Info("Force new migration, starting fresh")
		m.migration.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.migration.resuming = false

		// Drop the meta bucket and start fresh. At this point we can
		// be sure that the meta bucket exists.
		err = targetTx.DeleteTopLevelBucket(
			[]byte(migrationMetaBucket),
		)
		if err != nil {
			return fmt.Errorf("failed to delete meta bucket: %w",
				err)
		}

		if err := targetTx.Commit(); err != nil {
			return fmt.Errorf("failed to commit target tx: %w", err)
		}

		// Reopen the target transaction.
		targetTx, err = targetDB.BeginReadWriteTx()
		if err != nil {
			return fmt.Errorf("failed to begin target tx: %w", err)
		}

	default:
		// We make sure the chunk size remains because the verification
		// hashes are calculated chunk-wise and they will not match
		// otherwise.
		if m.migration.persistedState.ChunkSize != m.cfg.ChunkSize {
			return fmt.Errorf("chunk size mismatch, "+
				"previous migration chunk size was %d, "+
				"but current chunk size is %d",
				m.migration.persistedState.ChunkSize, m.cfg.ChunkSize)
		}

		if m.migration.persistedState.CurrentBucketPath == migrationCompleteMarker {
			m.cfg.Logger.Info("Migration already complete, " +
				"set the force new migration flag to start " +
				"fresh")

			return nil
		}
		m.migration.resuming = true

		m.cfg.Logger.InfoS(ctx, "Resuming migration",
			"bucket", m.migration.persistedState.CurrentBucketPath,
			"processed_keys", m.migration.persistedState.ProcessedKeys,
		)
	}

	// Process chunks until complete.
	for {
		m.cfg.Logger.Infof("Processing chunk...")
		err = m.processChunk(ctx, sourceTx, targetTx)
		if err == nil {
			m.cfg.Logger.Infof("Migration complete, processed "+
				"in total %d keys",
				m.migration.persistedState.ProcessedKeys)

			// If we reach this point we have successfully migrated
			// the entire source database. Therefore we update the
			// migration state to indicate that the migration is
			// complete.
			m.migration.markComplete()
			m.migration.write(targetTx)

			// Migration complete, commit final transactions.
			sourceTx.Rollback()
			if err := targetTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit final "+
					"target tx: %w", err)
			}

			if m.migration.resuming {
				return fmt.Errorf("no keys were migrated, " +
					"the migration state path was not " +
					"found")
			}

			// Verify the migration if not skipped.
			if !m.cfg.SkipVerification {
				return m.VerifyMigration(ctx, targetDB)
			}

			return nil
		}
		if err == errChunkSizeExceeded {
			if err := m.migration.write(targetTx); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			if err := m.recordChunkHash(targetTx); err != nil {
				return fmt.Errorf("failed to record chunk "+
					"hash: %w", err)
			}

			// Commit target and rollback source tx for this chunk.
			sourceTx.Rollback()
			if err := targetTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit target "+
					"tx: %w", err)
			}

			// We log the progress and save it to disk.
			m.cfg.Logger.InfoS(
				ctx, "Committed chunk successfully:",
				"bucket", m.migration.persistedState.CurrentBucketPath,
				"processed_keys", m.migration.persistedState.ProcessedKeys,
				"current_chunk_size(B)",
				m.migration.currentChunkBytes,
				"source_hash", m.migration.hash.Sum64(),
			)

			m.cfg.Logger.Infof("Migration progress saved, " +
				"resuming with next chunk")

			m.migration.newChunk()

			// We need to start a new transaction for the next chunk.
			// We also start a read-write for the read tx because we
			// neeed access to the sequence number off the bucket.
			sourceTx, err = sourceDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin source "+
					"tx: %w", err)
			}
			targetTx, err = targetDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin target "+
					"tx: %w", err)
			}

			continue
		}

		return err
	}
}

// processChunk processes a single chunk of the migration by walking through
// the nested bucket structure and migrating the key-value pairs up to the
// specified chunk size.
func (m *Migrator) processChunk(ctx context.Context,
	sourceTx, targetTx walletdb.ReadWriteTx) error {

	// We start the iteration by looping through the root buckets.
	err := sourceTx.ForEachBucket(func(rootBucket []byte) error {
		sourceRootBucket := sourceTx.ReadWriteBucket(rootBucket)

		// We create the root bucket if it does not exist.
		targetRootBucket, err := targetTx.CreateTopLevelBucket(
			rootBucket,
		)
		if err != nil {
			return fmt.Errorf("failed to create target root "+
				"bucket: %w", err)
		}

		if !m.migration.resuming {
			m.cfg.Logger.Infof("Migrating root bucket: %s",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket to the source
			// hash.
			m.migration.hash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path
		initialPath := hex.EncodeToString(rootBucket)
		err = m.migrateBucket(
			ctx, sourceRootBucket, targetRootBucket, initialPath,
		)
		if err == errChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return errChunkSizeExceeded
		}
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// migrateBucket migrates a bucket from the source database to the target
// database.
func (m *Migrator) migrateBucket(ctx context.Context,
	sourceB, targetB walletdb.ReadWriteBucket, path string) error {

	// Skip already migrated buckets in case we are resuming from a failed
	// migration or resuming from a new chunk.
	if m.migration.persistedState.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.migration.persistedState.CurrentBucketPath) {
		return nil
	}

	// Copying the sequence number over as well.
	if !m.migration.resuming {
		if err := targetB.SetSequence(sourceB.Sequence()); err != nil {
			return fmt.Errorf("error copying sequence number")
		}

		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, sourceB.Sequence())
		m.migration.hash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := sourceB.ReadCursor()

	// The key-value pair of the iterator.
	var k, v []byte

	// We check if this is the bucket we left off at.
	isResumptionPoint := path == m.migration.persistedState.CurrentBucketPath
	if isResumptionPoint {
		// We found the bucket where we left off, so we from now on
		// start processing the keys again.
		m.cfg.Logger.Infof("Resuming migration in bucket: %s", path)
		m.migration.resuming = false
	}

	// We also navigate to the last processed key if we are resuming in
	// this bucket.
	if isResumptionPoint && len(m.migration.persistedState.LastProcessedKey) > 0 {
		k, v = cursor.Seek(m.migration.persistedState.LastProcessedKey)
		if k == nil {
			return fmt.Errorf("failed to find last processed "+
				"key %x in bucket %s - database may be "+
				"corrupted", m.migration.persistedState.LastProcessedKey,
				path)
		}
		k, v = cursor.Next()
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

		// In case the value is nil this is a nested bucket so we go
		// into recursion here.
		if v == nil {
			sourceNestedBucket := sourceB.NestedReadWriteBucket(k)
			if sourceNestedBucket == nil {
				return fmt.Errorf("source nested bucket not "+
					"found for key %x", k)
			}
			targetNestedBucket := targetB.NestedReadWriteBucket(k)

			// If the target bucket does not exist we create it.
			// Because we do the migration in chunks we might have
			// already created this bucket int a previous chunk.
			if targetNestedBucket == nil {
				var err error
				targetNestedBucket, err = targetB.CreateBucket(k)
				if err != nil {
					return fmt.Errorf("failed to create "+
						"bucket %x: %w", k, err)
				}

				m.cfg.Logger.DebugS(ctx, "created nested bucket",
					"key", loggableKeyName(k),
					"value", loggableKeyName(v))
			}

			// We use the hex encoded key as the path to make sure
			// we don't have any special characters in the path.
			newPath := path
			if path == "" {
				newPath = hex.EncodeToString(k)
			} else {
				newPath += "/" + hex.EncodeToString(k)
			}

			if !m.migration.resuming {
				m.cfg.Logger.Debugf("Migrating a new nested "+
					"bucket: %s", newPath)

				// We add the hash of the nested bucket to the
				// source hash.
				m.migration.hash.Write(k)
			}

			// Recursively migrate the nested bucket
			if err := m.migrateBucket(
				ctx, sourceNestedBucket, targetNestedBucket,
				newPath,
			); err != nil {
				if err == errChunkSizeExceeded {
					// Propagate chunk size exceeded up the
					// stack.
					return err
				}
				return fmt.Errorf("failed to migrate bucket "+
					"%s: %w", path, err)
			}

			// We continue processing the other keys in the same
			// bucket.
			continue
		}

		// In case we are resuming from a new chunk or we recover from
		// a failed migration we skip the key processing. This will be
		// set to false once we either found the bucket or started with
		// a new migration.
		if m.migration.resuming {
			continue
		}

		m.cfg.Logger.DebugS(ctx, "migrating key-value pair:",
			"key", loggableKeyName(k),
			"value", loggableKeyName(v))

		// We copy the key-value pair to the target bucket but we are
		// not committing the transaction here. This will accumulate
		if err := targetB.Put(k, v); err != nil {
			return fmt.Errorf("failed to migrate key %x: %w", k,
				err)
		}

		// We record the stats of the migration and also log them when
		// writing the chunk to disk.

		m.migration.persistedState.ProcessedKeys++

		// We write the key value pairs to the source hash.
		m.migration.hash.Write(k)
		m.migration.hash.Write(v)

		m.cfg.Logger.TraceS(ctx, "new preliminary hash (migration)",
			"hash", m.migration.hash.Sum64())

		// If chunk size is reached, commit and signal pause
		entrySize := uint64(len(k) + len(v))
		m.migration.currentChunkBytes += entrySize

		if m.migration.currentChunkBytes >= m.cfg.ChunkSize {
			m.logChunkProgress(
				ctx, path, k, m.migration.currentChunkBytes,
				m.migration.hash.Sum64(),
			)

			// We reached the chunk size limit, so we update the
			// current bucket path and return the error so the
			// caller can handle the transaction commit/rollback.
			m.migration.persistedState.CurrentBucketPath = path
			m.migration.persistedState.LastProcessedKey = k
			m.migration.persistedState.LastChunkHash = m.migration.hash.Sum64()

			return errChunkSizeExceeded
		}
	}

	m.cfg.Logger.Debugf("Migration of bucket %s processing completed", path)

	return nil
}

// VerifyMigration verifies that the source and target databases match exactly.
// During the migration verification checksums are calculated while migrating
// through the source database. In this verification process we loop through the
// target database and create the checksums independently to compare them with
// the source checksums.
func (m *Migrator) VerifyMigration(ctx context.Context,
	targetDB walletdb.DB) error {

	targetTx, err := targetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	// We read the migration state, the meta bucket needs to exist and
	// the migration has to be complete.
	err = m.migration.read(targetTx)

	switch {
	case err != nil || m.migration.persistedState == nil:
		return fmt.Errorf("failed to read migration state: %w", err)

	case m.migration.persistedState.CurrentBucketPath != migrationCompleteMarker:
		return errMigrationIncomplete

	case m.migration.persistedState.ChunkSize != m.cfg.ChunkSize:
		return errChunkSizeMismatch

	case m.migration.persistedState.LastChunkHash == 0:
		return errNoChunkHash
	}

	err = m.verification.read(targetTx)
	switch {
	case err != nil && !errors.Is(err, errNoMetaBucket) &&
		!errors.Is(err, errNoStateFound):

		return fmt.Errorf("failed to read migration state: %w", err)

	case m.verification.persistedState == nil:
		// No state found, start fresh.
		m.cfg.Logger.Info("No verification state found, starting fresh")
		m.verification.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.verification.resuming = false

	case m.verification.persistedState.CurrentBucketPath == "":
		// State exists but empty path, start fresh.
		m.cfg.Logger.Error("Verification state found but empty path, " +
			"starting fresh")
		m.verification.persistedState = newPersistedState(m.cfg.ChunkSize)
		m.verification.resuming = false

	default:
		if m.verification.persistedState.CurrentBucketPath == verificationCompleteMarker {
			m.cfg.Logger.Info("Verification already complete")

			return nil
		}
		m.verification.resuming = true

		m.cfg.Logger.InfoS(ctx, "Resuming verification",
			"bucket", m.verification.persistedState.CurrentBucketPath,
			"processed_keys", m.verification.persistedState.ProcessedKeys,
		)
	}

	// Process chunks until complete.
	for {
		m.cfg.Logger.Infof("Processing verification chunk...")
		err = m.verifyChunk(ctx, targetTx)
		if err == nil {
			// We finished the verification process successfully and
			// we log the final state here.

			m.verification.markComplete()
			if err := m.verification.write(targetTx); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			// We commit the final transaction to have the updated
			// state.
			// TODO: Maybe we should keep a distinct write tx for
			// the changes to the meta bucket to show that we are
			// not changing something in the process of the
			// verification
			err = targetTx.Commit()
			if err != nil {
				return fmt.Errorf("failed to rollback target "+
					"tx: %w", err)
			}

			if m.verification.resuming {
				return fmt.Errorf("verification failed, " +
					"the verification state path was not " +
					"found")
			}

			sourceTx, err := targetDB.BeginReadTx()
			if err != nil {
				return fmt.Errorf("failed to begin source "+
					"tx: %w", err)
			}
			defer sourceTx.Rollback()

			err = m.migration.read(sourceTx)
			if err != nil {
				return fmt.Errorf("failed to read migration "+
					"state: %w", err)
			}

			if err := m.checkStatesMatch(); err != nil {
				return err
			}

			m.cfg.Logger.Infof("Verification complete, processed "+
				"in total %d keys",
				m.verification.persistedState.ProcessedKeys)

			// Now we also compare the processed keys and the last
			// chunk hash to make sure we verified the exact amount
			// of keys and the last hash is correct.

			return nil
		}
		if err == errChunkSizeExceeded {
			// We get the chunk hash for this exact bucket path.
			// We expect to find this path in the meta bucket from
			// the previous migration.
			lookupKey := append(
				[]byte(m.verification.persistedState.CurrentBucketPath),
				m.verification.persistedState.LastProcessedKey...,
			)
			migrationChunkHash, err := m.getChunkHash(
				targetTx,
				lookupKey,
			)
			if err != nil {
				return fmt.Errorf("failed to get chunk "+
					"hash: %w", err)
			}

			// We compare both chunk hashes if they are not the
			// same we return an error.
			if migrationChunkHash.Hash != m.verification.persistedState.LastChunkHash {
				return fmt.Errorf("chunk hash mismatch, "+
					"expected %d, got %d",
					migrationChunkHash.Hash,
					m.verification.persistedState.LastChunkHash)
			}

			m.cfg.Logger.Infof("Chunk hash match, verification "+
				"complete at path: %s",
				m.verification.persistedState.CurrentBucketPath)

			if err := m.verification.write(targetTx); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			m.cfg.Logger.Debugf("Verification state persisted: %v",
				m.verification.persistedState)

			// We reached the chunk size limit we rollback the
			// transaction.
			err = targetTx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit target "+
					"tx: %w", err)
			}

			m.cfg.Logger.Infof("Verification of chunk %s "+
				"complete", m.verification.persistedState.CurrentBucketPath)

			m.verification.newChunk()

			// We rolled back the transaction, so we need to start
			// a new one.
			targetTx, err = targetDB.BeginReadWriteTx()
			if err != nil {
				return fmt.Errorf("failed to begin target "+
					"tx: %w", err)
			}

			continue
		}

		return err

	}
}

func (m *Migrator) verifyChunk(ctx context.Context,
	targetTx walletdb.ReadWriteTx) error {

	// We start the iteration by looping through the root buckets.
	err := targetTx.ForEachBucket(func(rootBucket []byte) error {

		targetRootBucket := targetTx.ReadWriteBucket(rootBucket)
		if targetRootBucket == nil {
			return fmt.Errorf("failed to get target root bucket: %w",
				errNoBucket)
		}

		// We skip the migration meta bucket because it is not part of
		// the verification process.
		if bytes.Equal(rootBucket, []byte(migrationMetaBucket)) {
			return nil
		}

		if !m.verification.resuming {
			m.cfg.Logger.Infof("Verifying root bucket: %s",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket for the
			// verification process.
			m.verification.hash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path
		initialPath := hex.EncodeToString(rootBucket)
		err := m.verifyBucket(
			ctx, targetRootBucket, initialPath,
		)
		if err == errChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return errChunkSizeExceeded
		}
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

// verifyBucket loops through the bucket and creates the hash for the key-value
// pairs.
func (m *Migrator) verifyBucket(ctx context.Context,
	targetB walletdb.ReadWriteBucket, path string) error {

	// Skip already migrated buckets in case we are resuming from a failed
	// migration or resuming from a new chunk.
	if m.verification.persistedState.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.verification.persistedState.CurrentBucketPath) {
		return nil
	}

	// The sequence number is also part of the verification process.
	if !m.verification.resuming {
		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, targetB.Sequence())
		m.verification.hash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := targetB.ReadCursor()

	// The key-value pair of the iterator.
	var k, v []byte

	// We check if this is the bucket we left off at.
	isResumptionPoint := path == m.verification.persistedState.CurrentBucketPath
	if isResumptionPoint {
		// We found the bucket where we left off, so we from now on
		// start processing the keys again.
		m.cfg.Logger.Infof("Resuming migration in bucket: %s", path)
		m.verification.resuming = false
	}

	// We also navigate to the last processed key if we are resuming in
	// this bucket.
	if isResumptionPoint && len(m.verification.persistedState.LastProcessedKey) > 0 {
		k, v = cursor.Seek(m.verification.persistedState.LastProcessedKey)
		if k == nil {
			return fmt.Errorf("failed to find last processed "+
				"key %x in bucket %s - database may be "+
				"corrupted", m.verification.persistedState.LastProcessedKey,
				path)
		}
		k, v = cursor.Next()
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

		isNestedBucket := v == nil

		// This is needed because sqlite does treat an empty byte array
		// as nil and there might be keys which have an empty byte array
		// as value.
		var targetNestedBucket walletdb.ReadWriteBucket
		if isNestedBucket {
			targetNestedBucket = targetB.NestedReadWriteBucket(k)
			if targetNestedBucket == nil {
				m.cfg.Logger.Debugf("target nested bucket not "+
					"found for key, treating as key-value "+
					"pair %x", k)

				isNestedBucket = false
			}
		}

		// In case the value is nil this is a nested bucket so we go
		// into recursion here.
		switch {
		case isNestedBucket:
			// We use the hex encoded key as the path to make sure
			// we don't have any special characters in the path.
			newPath := path
			if path == "" {
				newPath = hex.EncodeToString(k)
			} else {
				newPath += "/" + hex.EncodeToString(k)
			}

			if !m.verification.resuming {
				m.cfg.Logger.Debugf("Verifying a new nested "+
					"bucket: %s", newPath)

				// We add the hash of the nested bucket to the
				// verification hash as well.
				m.verification.hash.Write(k)
			}

			// Recursively migrate the nested bucket.
			if err := m.verifyBucket(
				ctx, targetNestedBucket, newPath,
			); err != nil {
				if err == errChunkSizeExceeded {
					// Propagate chunk size exceeded up the
					// stack.
					return err
				}
				return fmt.Errorf("failed to verify bucket "+
					"%s: %w", path, err)
			}

		default:
			// In case we are resuming from a new chunk or we recover from
			// a failed migration we skip the key processing. This will be
			// set to false once we either found the bucket or started with
			// a new migration.
			if m.verification.resuming {
				continue
			}

			// We record the stats of the migration and also log them when
			// writing the chunk to disk.
			m.verification.persistedState.ProcessedKeys++

			// We write the key value pairs to the source hash.
			m.verification.hash.Write(k)
			m.verification.hash.Write(v)

			m.cfg.Logger.DebugS(ctx, "verifying key-value pair:",
				"key", loggableKeyName(k), "value", loggableKeyName(v))

			m.cfg.Logger.DebugS(ctx, "new preliminary hash (verification)",
				"hash", m.verification.hash.Sum64())

			// If chunk size is reached, commit and signal pause
			entrySize := uint64(len(k) + len(v))
			m.verification.currentChunkBytes += entrySize

			if m.verification.currentChunkBytes >= m.cfg.ChunkSize {
				m.logChunkProgress(
					ctx, path, k,
					m.verification.currentChunkBytes,
					m.verification.hash.Sum64(),
				)

				// We reached the chunk size limit, so we update the
				// current bucket path and return the error so the
				// caller can handle the transaction commit/rollback.
				m.verification.persistedState.CurrentBucketPath = path
				m.verification.persistedState.LastProcessedKey = k
				m.verification.persistedState.LastChunkHash = m.verification.hash.Sum64()

				return errChunkSizeExceeded
			}
		}

	}

	m.cfg.Logger.Debugf("Verification of bucket %s processing "+
		"completed", path)

	return nil
}

// checkStatesMatch compares migration and verification states and returns a detailed
// error if they don't match
func (m *Migrator) checkStatesMatch() error {
	migState := m.migration.persistedState
	verState := m.verification.persistedState

	if migState.ProcessedKeys == verState.ProcessedKeys &&
		migState.LastChunkHash == verState.LastChunkHash &&
		bytes.Equal(migState.LastProcessedKey, verState.LastProcessedKey) &&
		migState.ChunkSize == verState.ChunkSize {

		return nil
	}

	return fmt.Errorf("migration/verification state mismatch:\n"+
		"Migration State:\n"+
		"  Processed Keys: %d\n"+
		"  Last Chunk Hash: %d\n"+
		"  Current Bucket Path: %s\n"+
		"  Last Processed Key: %x\n"+
		"  Chunk Size: %d\n"+
		"Verification State:\n"+
		"  Processed Keys: %d\n"+
		"  Last Chunk Hash: %d\n"+
		"  Current Bucket Path: %s\n"+
		"  Last Processed Key: %x\n"+
		"  Chunk Size: %d",
		migState.ProcessedKeys,
		migState.LastChunkHash,
		migState.CurrentBucketPath,
		migState.LastProcessedKey,
		migState.ChunkSize,
		verState.ProcessedKeys,
		verState.LastChunkHash,
		verState.CurrentBucketPath,
		verState.LastProcessedKey,
		verState.ChunkSize,
	)
}
