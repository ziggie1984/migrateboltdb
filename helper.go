package migrateboltdb

import (
	"encoding/hex"
	"strings"

	"github.com/btcsuite/btcwallet/walletdb"
)

// loggableKeyName returns a string representation of a bucket key suitable for logging
func loggableKeyName(key []byte) string {
	// For known bucket names, return as string if printable ASCII
	if isPrintableASCII(key) {
		return string(key)
	}
	// Otherwise return hex encoding
	return hex.EncodeToString(key)
}

// hasSpecialChars returns true if any of the characters in the given string
// cannot be printed.
func isPrintableASCII(b []byte) bool {
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// isBucketMigrated checks if a bucket would have already been processed
// in our depth-first traversal based on the current path we're processing.
//
// NOTE: This is a BOLT-specific optimization and will not work for other
// databases very likely.
func isBucketMigrated(pathToCheck string,
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

// getOrCreateMetaBucket gets or creates the migration metadata bucket
func getOrCreateMetaBucket(tx walletdb.ReadWriteTx) (
	walletdb.ReadWriteBucket, error) {

	metaBucket := tx.ReadWriteBucket([]byte(migrationMetaBucket))
	if metaBucket != nil {
		return metaBucket, nil
	}

	return tx.CreateTopLevelBucket([]byte(migrationMetaBucket))
}
