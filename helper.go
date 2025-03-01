package migrateboltdb

import "encoding/hex"

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
