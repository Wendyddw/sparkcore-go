// Package plan defines the serializable computation metadata used to describe
// RDD lineage, dependencies, partitioning, and executable stage plans.
//
// The package is intentionally independent of the public API, scheduler, and
// executor so those layers can share planning types without import cycles.
// IDs are scoped to one driver process and are not stable across restarts.
package plan
