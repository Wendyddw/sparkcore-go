// Package scheduler converts validated RDD lineage into stages and tasks.
package scheduler

import "github.com/Wendyddw/sparkcore-go/plan"

// StageKind identifies a stage's role in a job.
type StageKind string

const (
	// StageShuffleMap produces partitioned shuffle output for child stages.
	StageShuffleMap StageKind = "shuffle_map"
	// StageResult produces the final action result.
	StageResult StageKind = "result"
)

// ActionKind identifies the terminal operation requested by a job.
type ActionKind string

const (
	// ActionCollect returns every record from the target RDD.
	ActionCollect ActionKind = "collect"
	// ActionCount returns the target RDD's record count.
	ActionCount ActionKind = "count"
)

// ActionSpec describes the terminal operation attached to a result stage.
type ActionSpec struct {
	Kind      ActionKind `json:"kind"`
	TargetRDD plan.RDDID `json:"target_rdd"`
}

// ShuffleWriteSpec describes shuffle output produced by a shuffle-map stage.
type ShuffleWriteSpec struct {
	ShuffleID      plan.ShuffleID       `json:"shuffle_id"`
	Partitioner    plan.PartitionerSpec `json:"partitioner"`
	AggregatorID   string               `json:"aggregator_id"`
	MapSideCombine bool                 `json:"map_side_combine"`
}

// Stage is one execution unit bounded by shuffle dependencies.
type Stage struct {
	ID            plan.StageID      `json:"id"`
	Kind          StageKind         `json:"kind"`
	ParentIDs     []plan.StageID    `json:"parent_ids,omitempty"`
	NumPartitions int               `json:"num_partitions"`
	ShuffleWrite  *ShuffleWriteSpec `json:"shuffle_write,omitempty"`
	FinalAction   *ActionSpec       `json:"final_action,omitempty"`
}

// StagePlan contains a job's stages in parent-before-child order.
type StagePlan struct {
	TargetRDD plan.RDDID `json:"target_rdd"`
	Stages    []Stage    `json:"stages"`
}
