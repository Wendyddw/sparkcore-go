package scheduler

import "github.com/Wendyddw/sparkcore-go/plan"

// StageOperationKind identifies an executable step inside a stage.
type StageOperationKind string

const (
	// StageOperationRDD executes one RDD node's operator.
	StageOperationRDD StageOperationKind = "rdd"
	// StageOperationShuffleRead consumes partitioned output from a parent stage.
	StageOperationShuffleRead StageOperationKind = "shuffle_read"
)

// RDDOperationSpec identifies an RDD operator in a stage pipeline.
type RDDOperationSpec struct {
	RDDID    plan.RDDID        `json:"rdd_id"`
	Operator plan.OperatorSpec `json:"operator"`
}

// ShuffleReadSpec describes shuffle input consumed by a child stage.
type ShuffleReadSpec struct {
	ShuffleID   plan.ShuffleID       `json:"shuffle_id"`
	Partitioner plan.PartitionerSpec `json:"partitioner"`
}

// StageOperation is one source-to-output step in a stage pipeline.
// Exactly one kind-specific specification is set by the stage planner.
type StageOperation struct {
	Kind        StageOperationKind `json:"kind"`
	RDD         *RDDOperationSpec  `json:"rdd,omitempty"`
	ShuffleRead *ShuffleReadSpec   `json:"shuffle_read,omitempty"`
}
