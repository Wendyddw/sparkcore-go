package plan

// DependencyKind identifies how a child RDD depends on its parent.
type DependencyKind string

const (
	// DependencyNarrow keeps parent and child partitions aligned.
	DependencyNarrow DependencyKind = "narrow"
	// DependencyShuffle redistributes records across partitions.
	DependencyShuffle DependencyKind = "shuffle"
)

// NarrowMapping identifies how child partitions select parent partitions.
type NarrowMapping string

const (
	// NarrowOneToOne maps each child partition to the same parent partition.
	NarrowOneToOne NarrowMapping = "one_to_one"
)

// NarrowDependencySpec describes a narrow partition mapping.
type NarrowDependencySpec struct {
	Mapping NarrowMapping `json:"mapping"`
}

// ShuffleDependencySpec describes a shuffle boundary.
type ShuffleDependencySpec struct {
	ShuffleID      ShuffleID       `json:"shuffle_id"`
	Partitioner    PartitionerSpec `json:"partitioner"`
	AggregatorID   string          `json:"aggregator_id"`
	MapSideCombine bool            `json:"map_side_combine"`
}

// Dependency links an RDD node to one parent.
// Exactly one kind-specific specification must be present.
type Dependency struct {
	Kind     DependencyKind         `json:"kind"`
	ParentID RDDID                  `json:"parent_id"`
	Narrow   *NarrowDependencySpec  `json:"narrow,omitempty"`
	Shuffle  *ShuffleDependencySpec `json:"shuffle,omitempty"`
}
