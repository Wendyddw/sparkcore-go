package scheduler

import "github.com/Wendyddw/sparkcore-go/plan"

// Task is the stable logical work for one stage partition.
// Retries create new attempt identities without changing this task.
type Task struct {
	ID            plan.TaskID       `json:"id"`
	StageID       plan.StageID      `json:"stage_id"`
	StageKind     StageKind         `json:"stage_kind"`
	PartitionID   plan.PartitionID  `json:"partition_id"`
	NumPartitions int               `json:"num_partitions"`
	Operations    []StageOperation  `json:"operations"`
	ShuffleWrite  *ShuffleWriteSpec `json:"shuffle_write,omitempty"`
	FinalAction   *ActionSpec       `json:"final_action,omitempty"`
}

// TaskAttemptIdentity identifies one physical attempt of a logical task.
// Attempt state and retry policy are introduced in a later session.
type TaskAttemptIdentity struct {
	ID             plan.TaskAttemptID  `json:"id"`
	TaskID         plan.TaskID         `json:"task_id"`
	StageAttemptID plan.StageAttemptID `json:"stage_attempt_id"`
}
