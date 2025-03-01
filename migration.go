package migrateboltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/btcsuite/btcwallet/walletdb"
)

// ErrChunkSizeExceeded is returned when the chunk size limit is reached during
// migration, indicating that the migration should continue with a new
// transaction. It should close the reading and write transaction and continue
// where it stopped.
var (
	ErrNoMetaBucket      = fmt.Errorf("migration metadata bucket not found")
	ErrNoStateFound      = fmt.Errorf("no migration state found")
	ErrChunkSizeExceeded = fmt.Errorf("chunk size exceeded")
	ErrNoChunkHash       = fmt.Errorf("no chunk hash found")
	ErrNoBucket          = fmt.Errorf("no bucket found")
)

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

// ChunkHash represents the verification hash of a chunk of the source database.
type ChunkHash struct {
	Hash    uint64 `json:"hash"`
	LastKey []byte `json:"last_key"`
	Path    string `json:"path"`
}

// getOrCreateMetaBucket gets or creates the migration metadata bucket
func (m *Migrator) getOrCreateMetaBucket(tx walletdb.ReadWriteTx) (
	walletdb.ReadWriteBucket, error) {

	metaBucket := tx.ReadWriteBucket([]byte(migrationMetaBucket))
	if metaBucket != nil {
		return metaBucket, nil
	}

	return tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
}

// Record chunk hash in target DB.
func (m *Migrator) recordChunkHash(targetTx walletdb.ReadWriteTx) error {
	metaBucket, err := m.getOrCreateMetaBucket(targetTx)
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	record := ChunkHash{
		Hash:    m.sourceHash.Sum64(),
		LastKey: m.migrationState.LastProcessedKey,
		Path:    m.migrationState.CurrentBucketPath,
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Create a unique key by combining bucket path and last processed key
	key := append(
		[]byte(m.migrationState.CurrentBucketPath),
		m.migrationState.LastProcessedKey...,
	)
	err = metaBucket.Put(key, encoded)

	return err
}

// getChunkHash tries to get the chunk hash from a specific bucket path.
func (m *Migrator) getChunkHash(targetTx walletdb.ReadTx,
	lookupKey []byte) (ChunkHash, error) {

	metaBucket := targetTx.ReadBucket([]byte(migrationMetaBucket))
	if metaBucket == nil {
		return ChunkHash{}, ErrNoMetaBucket
	}

	chunkHashBytes := metaBucket.Get(lookupKey)
	if chunkHashBytes == nil {
		return ChunkHash{}, ErrNoChunkHash
	}

	var chunkHash ChunkHash
	if err := json.Unmarshal(chunkHashBytes, &chunkHash); err != nil {
		return ChunkHash{}, fmt.Errorf("failed to unmarshal chunk hash: %w", err)
	}

	return chunkHash, nil
}

// Update state in target DB
func (m *Migrator) updateMigrationState(targetTx walletdb.ReadWriteTx) error {
	metaBucket, err := m.getOrCreateMetaBucket(targetTx)
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	state := state{
		CurrentBucketPath: m.migrationState.CurrentBucketPath,
		LastProcessedKey:  m.migrationState.LastProcessedKey,
		ProcessedKeys:     m.migrationState.ProcessedKeys,
		LastChunkHash:     m.migrationState.LastChunkHash,
		Timestamp:         time.Now(),
		ChunkSize:         m.cfg.ChunkSize,
	}

	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return metaBucket.Put([]byte(migrationStateKey), encoded)
}

// ReadMigrationState reads the current migration state from the target DB.
func (m *Migrator) readMigrationState(
	targetTx walletdb.ReadTx) (*state, error) {

	metaBucket := targetTx.ReadBucket([]byte(migrationMetaBucket))
	if metaBucket == nil {
		// No state yet, return empty state
		return nil, ErrNoMetaBucket
	}

	stateBytes := metaBucket.Get([]byte(migrationStateKey))
	if stateBytes == nil {
		// No state saved yet, return empty state
		return nil, ErrNoStateFound
	}

	var state state
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// readVerificationState reads the current verification state from the db.
func (m *Migrator) readVerificationState(
	targetTx walletdb.ReadTx) (*state, error) {

	metaBucket := targetTx.ReadBucket([]byte(migrationMetaBucket))
	if metaBucket == nil {
		return nil, ErrNoMetaBucket
	}

	stateBytes := metaBucket.Get([]byte(verificationStateKey))
	if stateBytes == nil {
		return nil, ErrNoStateFound
	}

	var state state
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// updateVerificationState updates the verification state in the db.
func (m *Migrator) updateVerificationState(targetTx walletdb.ReadWriteTx) error {
	metaBucket, err := m.getOrCreateMetaBucket(targetTx)
	if err != nil {
		return fmt.Errorf("failed to get meta bucket: %w", err)
	}

	state := state{
		CurrentBucketPath: m.verificationState.CurrentBucketPath,
		LastProcessedKey:  m.verificationState.LastProcessedKey,
		ProcessedKeys:     m.verificationState.ProcessedKeys,
		LastChunkHash:     m.verificationState.LastChunkHash,
		Timestamp:         time.Now(),
		ChunkSize:         m.cfg.ChunkSize,
	}

	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return metaBucket.Put([]byte(verificationStateKey), encoded)
}

// TODO:
// RESTRICT Migration only to BBOLT => SQL because we are leveraging the fact
// that bolt db loops through keys in lexicographical order which cannot be
// guaranteed for other databases.

// Config holds the configuration for the migrator.
type Config struct {
	// ChunkSize is the number of items (key-value pairs or buckets) to
	// process in a single transaction.
	ChunkSize uint64

	// Logger is the logger to use for logging.
	Logger btclog.Logger

	// SourceDB is the source database to migrate from.
	SourceDB walletdb.DB

	// TargetDB is the target database to migrate to.
	TargetDB walletdb.DB

	// ForceNewMigration is a flag to force a new migration even if a
	// migration state is found.
	ForceNewMigration bool
}

// state tracks the migration progress for resumability.
type state struct {
	// Currently processing bucket
	CurrentBucketPath string `json:"current_bucket_path"`

	// Last key processed in current bucket
	LastProcessedKey []byte `json:"last_processed_key"`

	// Number of keys processed
	ProcessedKeys int64 `json:"processed_keys"`

	// Timestamp of the migration
	Timestamp time.Time `json:"timestamp"`

	// Hash of the last chunk
	LastChunkHash uint64 `json:"last_chunk_hash"`

	// We need to make sure that the chunk size remains the same during
	// the complete migration, because we are also creating verification
	// hahses for each chunk we later might compare therefore the size
	// needs to be constant.
	ChunkSize uint64 `json:"chunk_size"`
}

// Migrator handles the chunked migration of a bolt database.
type Migrator struct {
	// cfg is the configuration for the migrator.
	cfg Config

	// migrationState tracks the migration progress for resumability.
	migrationState *state

	// verificationState tracks the verification progress for resumability.
	verificationState *state

	// currentChunkSize is the size of the current chunk.
	currentChunkSize uint64

	// When resuming a migration, we need to skip processing keys until we
	// find the bucket where we left off. This flag controls that skipping
	// behavior during the recursive bucket traversal.
	resuming bool

	// sourceHash is used to calucalte the hash of the source database.
	sourceHash hash.Hash64

	// targetHash is used to calucalte the hash of the target database.
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
		cfg:               cfg,
		migrationState:    &state{},
		verificationState: &state{},
		sourceHash:        fnv.New64(),
		targetHash:        fnv.New64(),
	}, nil
}

// isBucketMigrated checks if a bucket would have already been processed
// in our depth-first traversal based on the current path we're processing.
//
// NOTE: This is a BOLT-specific optimization and will not work for other
// databases.
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

// Migrate performs the migration of the source database to the target database.
func (m *Migrator) Migrate(ctx context.Context) error {
	m.cfg.Logger.Infof("Migrating database ...")

	// First we need to get all the buckets from the source database.
	sourceTx, err := m.cfg.SourceDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin source tx: %w", err)
	}
	defer sourceTx.Rollback()

	// Open also a target transaction for the migration process.
	targetTx, err := m.cfg.TargetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	migrationState, err := m.readMigrationState(targetTx)
	switch {
	case err != nil && !errors.Is(err, ErrNoMetaBucket) &&
		!errors.Is(err, ErrNoStateFound):

		return fmt.Errorf("failed to read migration state: %w", err)

	case migrationState == nil:
		// No state found, start fresh.
		m.cfg.Logger.Info("No migration state found, starting fresh")
		m.migrationState = &state{}
		m.resuming = false

	case migrationState.CurrentBucketPath == "":
		// State exists but empty path, start fresh.
		m.cfg.Logger.Error("Migration state found but empty path, " +
			"starting fresh")
		m.migrationState = &state{}
		m.resuming = false

	case m.cfg.ForceNewMigration:
		// Force new migration, start fresh.
		m.cfg.Logger.Info("Force new migration, starting fresh")
		m.migrationState = &state{}
		m.resuming = false

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
		targetTx, err = m.cfg.TargetDB.BeginReadWriteTx()
		if err != nil {
			return fmt.Errorf("failed to begin target tx: %w", err)
		}

	default:
		// We make sure the chunk size remains because the verification
		// hashes are calculated chunk-wise and they will not match
		// otherwise.
		if migrationState.ChunkSize != m.cfg.ChunkSize {
			return fmt.Errorf("chunk size mismatch, "+
				"previous migration chunk size was %d, "+
				"but current chunk size is %d",
				migrationState.ChunkSize, m.cfg.ChunkSize)
		}

		if migrationState.CurrentBucketPath == "migration_complete" {
			m.cfg.Logger.Info("Migration already complete, " +
				"forcing new migration to start fresh")

			return nil
		}
		m.migrationState = migrationState
		m.resuming = true

		m.cfg.Logger.InfoS(ctx, "Resuming migration",
			"bucket", migrationState.CurrentBucketPath,
			"processed_keys", migrationState.ProcessedKeys,
		)
	}

	// Process chunks until complete.
	for {
		m.cfg.Logger.Infof("Processing chunk...")
		err = m.processChunk(ctx, sourceTx, targetTx)
		if err == nil {
			m.cfg.Logger.Infof("Migration complete, processed "+
				"in total %d keys", m.migrationState.ProcessedKeys)

			// If we reach this point we have successfully migrated
			// the entire source database. Therefore we update the
			// migration state to indicate that the migration is
			// complete.
			m.migrationState.CurrentBucketPath = "migration_complete"
			m.migrationState.LastProcessedKey = nil
			m.migrationState.Timestamp = time.Now()
			m.migrationState.LastChunkHash = m.sourceHash.Sum64()
			m.currentChunkSize = 0
			m.updateMigrationState(targetTx)

			// Migration complete, commit final transactions.
			sourceTx.Rollback()
			if err := targetTx.Commit(); err != nil {
				return fmt.Errorf("failed to commit final "+
					"target tx: %w", err)
			}

			if m.resuming {
				return fmt.Errorf("no keys were migrated, " +
					"the migration state path was not " +
					"found")
			}

			return nil
		}
		if err == ErrChunkSizeExceeded {
			if err := m.updateMigrationState(targetTx); err != nil {
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
				"bucket", m.migrationState.CurrentBucketPath,
				"processed_keys", m.migrationState.ProcessedKeys,
				"current_chunk_size(B)",
				m.currentChunkSize,
				"source_hash", m.sourceHash.Sum64(),
			)

			m.cfg.Logger.Infof("Migration progress saved, " +
				"resuming with next chunk")

			// Reset for next chunk
			m.currentChunkSize = 0

			// We migrate the current chunk, so will close the
			// db transactions, this variable will make sure we only
			// start migrating the keys until we found the bucket
			// we left off at.
			m.resuming = true

			// Reset the source hash for the next chunk. We cannot
			// calculate an overall hash because we cannot easily
			// recover the state of the source hash, so we make sure
			// we use the same chunk size during the whole
			// migration.
			m.sourceHash.Reset()

			// We need to start a new transaction for the next chunk.
			// We also start a read-write for the read tx because we
			// neeed access to the sequence number off the bucket.
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

		if !m.resuming {
			m.cfg.Logger.Infof("Migrating root bucket: %s",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket to the source
			// hash.
			m.sourceHash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path
		initialPath := hex.EncodeToString(rootBucket)
		err = m.migrateBucket(
			ctx, sourceRootBucket, targetRootBucket, initialPath,
		)
		if err == ErrChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return ErrChunkSizeExceeded
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
	if m.migrationState.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.migrationState.CurrentBucketPath) {
		return nil
	}

	// Copying the sequence number over as well.
	if !m.resuming {
		if err := targetB.SetSequence(sourceB.Sequence()); err != nil {
			return fmt.Errorf("error copying sequence number")
		}

		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, sourceB.Sequence())
		m.sourceHash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := sourceB.ReadCursor()

	// The key-value pair of the iterator.
	var k, v []byte

	// We check if this is the bucket we left off at.
	isResumptionPoint := path == m.migrationState.CurrentBucketPath
	if isResumptionPoint {
		// We found the bucket where we left off, so we from now on
		// start processing the keys again.
		m.cfg.Logger.Infof("Resuming migration in bucket: %s", path)
		m.resuming = false
	}

	// We also navigate to the last processed key if we are resuming in
	// this bucket.
	if isResumptionPoint && len(m.migrationState.LastProcessedKey) > 0 {
		k, v = cursor.Seek(m.migrationState.LastProcessedKey)
		if k == nil {
			return fmt.Errorf("failed to find last processed "+
				"key %x in bucket %s - database may be "+
				"corrupted", m.migrationState.LastProcessedKey,
				path)
		}
		if bytes.Equal(k, m.migrationState.LastProcessedKey) {
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

			if !m.resuming {
				m.cfg.Logger.Infof("Migrating a new nested "+
					"bucket: %s", newPath)

				// We add the hash of the nested bucket to the
				// source hash.
				m.sourceHash.Write(k)
			}

			// Recursively migrate the nested bucket
			if err := m.migrateBucket(
				ctx, sourceNestedBucket, targetNestedBucket,
				newPath,
			); err != nil {
				if err == ErrChunkSizeExceeded {
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
		if m.resuming {
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

		m.migrationState.ProcessedKeys++

		// We write the key value pairs to the source hash.
		m.sourceHash.Write(k)
		m.sourceHash.Write(v)

		m.cfg.Logger.TraceS(ctx, "new preliminary hash (migration)",
			"hash", m.sourceHash.Sum64())

		// If chunk size is reached, commit and signal pause
		entrySize := uint64(len(k) + len(v))
		m.currentChunkSize += entrySize

		if m.currentChunkSize >= m.cfg.ChunkSize {
			m.cfg.Logger.InfoS(ctx, "Chunk size exceeded, committing:",
				"current_bucket", path,
				"last_key", loggableKeyName(k),
				"chunk_size(allowed bytes)", m.cfg.ChunkSize,
				"chunk_size(processed bytes)",
				m.currentChunkSize,
				"source_hash", m.sourceHash.Sum64(),
			)

			// We reached the chunk size limit, so we update the
			// current bucket path and return the error so the
			// caller can handle the transaction commit/rollback.
			m.migrationState.CurrentBucketPath = path
			m.migrationState.LastProcessedKey = k
			m.migrationState.LastChunkHash = m.sourceHash.Sum64()

			return ErrChunkSizeExceeded
		}
	}

	m.cfg.Logger.Infof("Migration of bucket %s processing "+
		"completed", path)

	return nil
}

// VerifyMigration verifies that the source and target databases match exactly.
// During the migration verification checksums are calculated while migrating
// through the source database. In this verification process we loop through the
// target database and create the checksums independently to compare them with
// the source checksums.
func (m *Migrator) VerifyMigration(ctx context.Context) error {
	// TODO: Maybe move this variable in the verification state so we do
	// not need to think about this here.
	m.currentChunkSize = 0

	targetTx, err := m.cfg.TargetDB.BeginReadWriteTx()
	if err != nil {
		return fmt.Errorf("failed to begin target tx: %w", err)
	}
	defer targetTx.Rollback()

	// We read the migration state, the meta bucket needs to exist and
	// the migration has to be complete.
	migrationState, err := m.readMigrationState(targetTx)
	if err != nil {
		return fmt.Errorf("failed to read migration state: %w", err)
	}

	switch {
	case err != nil || migrationState == nil:
		return fmt.Errorf("failed to read migration state: %w", err)

	case migrationState.CurrentBucketPath != "migration_complete":
		// return fmt.Errorf("migration not completed verification " +
		// 	"failed")

	case migrationState.ChunkSize != m.cfg.ChunkSize:
		return fmt.Errorf("Chunk size mismatch, migration chunk size "+
			"was %d, but verification chunk size is %d, they "+
			"have to be the same", migrationState.ChunkSize,
			m.cfg.ChunkSize)

	case migrationState.LastChunkHash == 0:
		return fmt.Errorf("migration hash not found verification " +
			"failed")
	}

	// We read the verification state.
	verificationState, err := m.readVerificationState(targetTx)
	switch {
	case err != nil && !errors.Is(err, ErrNoMetaBucket) &&
		!errors.Is(err, ErrNoStateFound):

		return fmt.Errorf("failed to read migration state: %w", err)

	case verificationState == nil:
		// No state found, start fresh.
		m.cfg.Logger.Info("No verification state found, starting fresh")
		m.verificationState = &state{}
		m.resuming = false

	case verificationState.CurrentBucketPath == "":
		// State exists but empty path, start fresh.
		m.cfg.Logger.Error("Verification state found but empty path, " +
			"starting fresh")
		m.verificationState = &state{}
		m.resuming = false

	default:
		if verificationState.CurrentBucketPath == "verification_complete" {
			m.cfg.Logger.Info("Verification already complete")

			return nil
		}
		m.verificationState = verificationState
		m.resuming = true

		m.cfg.Logger.InfoS(ctx, "Resuming verification",
			"bucket", verificationState.CurrentBucketPath,
			"processed_keys", verificationState.ProcessedKeys,
		)
	}

	// Process chunks until complete.
	for {
		m.cfg.Logger.Infof("Processing verification chunk...")
		err = m.verifyChunk(ctx, targetTx)
		if err == nil {
			// We finished the verification process successfully and
			// we log the final state here.

			m.verificationState.CurrentBucketPath = "verification_complete"
			m.verificationState.Timestamp = time.Now()
			m.verificationState.LastChunkHash = m.targetHash.Sum64()
			m.updateVerificationState(targetTx)

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

			if m.resuming {
				return fmt.Errorf("verification failed, " +
					"the verification state path was not " +
					"found")
			}

			sourceTx, err := m.cfg.SourceDB.BeginReadTx()
			if err != nil {
				return fmt.Errorf("failed to begin source tx: %w", err)
			}
			defer sourceTx.Rollback()

			migrationState, err := m.readMigrationState(sourceTx)
			if err != nil {
				return fmt.Errorf("failed to read migration state: %w", err)
			}

			if !m.statesMatch(migrationState) {
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
					migrationState.ProcessedKeys,
					migrationState.LastChunkHash,
					migrationState.CurrentBucketPath,
					migrationState.LastProcessedKey,
					migrationState.ChunkSize,
					m.verificationState.ProcessedKeys,
					m.verificationState.LastChunkHash,
					m.verificationState.CurrentBucketPath,
					m.verificationState.LastProcessedKey,
					m.verificationState.ChunkSize,
				)
			}

			m.cfg.Logger.Infof("Verification complete, processed "+
				"in total %d keys", m.verificationState.ProcessedKeys)

			// Now we also compare the processed keys and the last
			// chunk hash to make sure we verified the exact amount
			// of keys and the last hash is correct.

			return nil
		}
		if err == ErrChunkSizeExceeded {
			// We get the chunk hash for this exact bucket path.
			// We expect to find this path in the meta bucket from
			// the previous migration.
			lookupKey := append(
				[]byte(m.verificationState.CurrentBucketPath),
				m.verificationState.LastProcessedKey...,
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
			if migrationChunkHash.Hash != m.verificationState.LastChunkHash {
				return fmt.Errorf("chunk hash mismatch, "+
					"expected %d, got %d",
					migrationChunkHash.Hash,
					m.verificationState.LastChunkHash)
			}

			m.cfg.Logger.Infof("Chunk hash match, verification "+
				"complete at path: %s",
				m.verificationState.CurrentBucketPath)

			if err := m.updateVerificationState(targetTx); err != nil {
				return fmt.Errorf("failed to save migration "+
					"state: %w", err)
			}

			m.cfg.Logger.Debugf("Verification state persisted: %v",
				m.verificationState)

			// We reached the chunk size limit we rollback the
			// transaction.
			err = targetTx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit target "+
					"tx: %w", err)
			}

			m.cfg.Logger.Infof("Verification of chunk %s "+
				"complete", m.verificationState.CurrentBucketPath)

			// Reset for next chunk
			m.currentChunkSize = 0

			// We resume the verification at this point.
			m.resuming = true

			// Reset the target hash after each chunk verfication.
			m.targetHash.Reset()

			// We rolled back the transaction, so we need to start
			// a new one.
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

func (m *Migrator) verifyChunk(ctx context.Context,
	targetTx walletdb.ReadWriteTx) error {

	// We start the iteration by looping through the root buckets.
	err := targetTx.ForEachBucket(func(rootBucket []byte) error {

		targetRootBucket := targetTx.ReadWriteBucket(rootBucket)
		if targetRootBucket == nil {
			return fmt.Errorf("failed to get target root bucket: %w",
				ErrNoBucket)
		}

		// We skip the migration meta bucket because it is not part of
		// the verification process.
		if bytes.Equal(rootBucket, []byte(migrationMetaBucket)) {
			return nil
		}

		if !m.resuming {
			m.cfg.Logger.Infof("Verifying root bucket: %s",
				loggableKeyName(rootBucket))

			// We also add the key of the root bucket for the
			// verification process.
			m.targetHash.Write(rootBucket)
		}

		// Start with the root bucket name as the initial path
		initialPath := hex.EncodeToString(rootBucket)
		err := m.verifyBucket(
			ctx, targetRootBucket, initialPath,
		)
		if err == ErrChunkSizeExceeded {
			// We return the error so the caller can handle the
			// transaction commit/rollback.
			return ErrChunkSizeExceeded
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
	if m.verificationState.CurrentBucketPath != "" &&
		m.isBucketMigrated(path, m.verificationState.CurrentBucketPath) {
		return nil
	}

	// The sequence number is also part of the verification process.
	if !m.resuming {
		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, targetB.Sequence())
		m.targetHash.Write(seqBytes)
	}

	// We now iterate over the source bucket.
	cursor := targetB.ReadCursor()

	// The key-value pair of the iterator.
	var k, v []byte

	// We check if this is the bucket we left off at.
	isResumptionPoint := path == m.verificationState.CurrentBucketPath
	if isResumptionPoint {
		// We found the bucket where we left off, so we from now on
		// start processing the keys again.
		m.cfg.Logger.Infof("Resuming migration in bucket: %s", path)
		m.resuming = false
	}

	// We also navigate to the last processed key if we are resuming in
	// this bucket.
	if isResumptionPoint && len(m.verificationState.LastProcessedKey) > 0 {
		k, v = cursor.Seek(m.verificationState.LastProcessedKey)
		if k == nil {
			return fmt.Errorf("failed to find last processed "+
				"key %x in bucket %s - database may be "+
				"corrupted", m.verificationState.LastProcessedKey,
				path)
		}
		if bytes.Equal(k, m.verificationState.LastProcessedKey) {
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

		isNestedBucket := v == nil

		// This is needed because sqlite does treat an empty byte array
		// as nil and there might be keys which have an empty byte array
		// as value.
		if isNestedBucket {
			targetNestedBucket := targetB.NestedReadWriteBucket(k)
			if targetNestedBucket == nil {
				m.cfg.Logger.Debugf("target nested bucket not "+
					"found for key treating as key-value "+
					"pair %x", k)

				isNestedBucket = false
			}

		}

		// In case the value is nil this is a nested bucket so we go
		// into recursion here.
		switch {
		case isNestedBucket:
			targetNestedBucket := targetB.NestedReadWriteBucket(k)
			if targetNestedBucket == nil {
				m.cfg.Logger.Debugf("target nested bucket not "+
					"found for key treating as key-value "+
					"pair %x", k)

				break
			}

			// We use the hex encoded key as the path to make sure
			// we don't have any special characters in the path.
			newPath := path
			if path == "" {
				newPath = hex.EncodeToString(k)
			} else {
				newPath += "/" + hex.EncodeToString(k)
			}

			if !m.resuming {
				m.cfg.Logger.Infof("Verifying a new nested "+
					"bucket: %s", newPath)

				// We add the hash of the nested bucket to the
				// verification hash as well.
				m.targetHash.Write(k)
			}

			// Recursively migrate the nested bucket
			if err := m.verifyBucket(
				ctx, targetNestedBucket, newPath,
			); err != nil {
				if err == ErrChunkSizeExceeded {
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
			if m.resuming {
				continue
			}

			// We record the stats of the migration and also log them when
			// writing the chunk to disk.
			m.verificationState.ProcessedKeys++

			// We write the key value pairs to the source hash.
			m.targetHash.Write(k)
			m.targetHash.Write(v)

			m.cfg.Logger.DebugS(ctx, "verifying key-value pair:",
				"key", loggableKeyName(k), "value", loggableKeyName(v))

			m.cfg.Logger.DebugS(ctx, "new preliminary hash (verification)",
				"hash", m.targetHash.Sum64())

			// If chunk size is reached, commit and signal pause
			entrySize := uint64(len(k) + len(v))
			m.currentChunkSize += entrySize

			if m.currentChunkSize >= m.cfg.ChunkSize {
				m.cfg.Logger.InfoS(ctx, "Chunk size exceeded, committing:",
					"current_bucket", path,
					"last_key", loggableKeyName(k),
					"chunk_size(allowed bytes)", m.cfg.ChunkSize,
					"chunk_size(processed bytes)",
					m.currentChunkSize,
					"target_hash", m.targetHash.Sum64(),
				)

				// We reached the chunk size limit, so we update the
				// current bucket path and return the error so the
				// caller can handle the transaction commit/rollback.
				m.verificationState.CurrentBucketPath = path
				m.verificationState.LastProcessedKey = k
				m.verificationState.LastChunkHash = m.targetHash.Sum64()

				return ErrChunkSizeExceeded
			}
		}

	}

	m.cfg.Logger.Infof("Verification of bucket %s processing "+
		"completed", path)

	return nil
}

// statesMatch checks if migration and verification states match
func (m *Migrator) statesMatch(migrationState *state) bool {
	return migrationState.ProcessedKeys == m.verificationState.ProcessedKeys &&
		migrationState.LastChunkHash == m.verificationState.LastChunkHash &&
		migrationState.CurrentBucketPath == m.verificationState.CurrentBucketPath &&
		bytes.Equal(migrationState.LastProcessedKey, m.verificationState.LastProcessedKey) &&
		migrationState.ChunkSize == m.verificationState.ChunkSize
}
