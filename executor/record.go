// Package executor defines records, registered functions, and partition-local
// execution primitives.
package executor

// Record is an MVP data value exchanged between operators.
// Values must be JSON-compatible so they can cross process boundaries later.
type Record any

// KeyValue is a keyed record used by pair and aggregation operators.
// Keys are strings in the MVP to keep hashing and transport deterministic.
type KeyValue struct {
	Key   string `json:"key"`
	Value Record `json:"value"`
}
