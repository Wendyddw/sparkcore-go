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
// TaskAttempt carries assignment state; retry policy is deferred to Week 3.
type TaskAttemptIdentity struct {
	ID             plan.TaskAttemptID  `json:"id"`
	TaskID         plan.TaskID         `json:"task_id"`
	StageAttemptID plan.StageAttemptID `json:"stage_attempt_id"`
}

// TaskState describes the physical scheduling state of a logical task.
type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskRunning   TaskState = "running"
	TaskSucceeded TaskState = "succeeded"
	TaskFailed    TaskState = "failed"
)

// TaskAttempt is one physical assignment of a logical task to a worker.
type TaskAttempt struct {
	Identity TaskAttemptIdentity `json:"identity"`
	Task     Task                `json:"task"`
	WorkerID plan.WorkerID       `json:"worker_id"`
	State    TaskState           `json:"state"`
}

// TaskAttemptSuccess reports partition output from one physical attempt.
type TaskAttemptSuccess struct {
	JobID       plan.JobID          `json:"job_id"`
	StageID     plan.StageID        `json:"stage_id"`
	Attempt     TaskAttemptIdentity `json:"attempt"`
	PartitionID plan.PartitionID    `json:"partition_id"`
	WorkerID    plan.WorkerID       `json:"worker_id"`
	Output      TaskOutput          `json:"output"`
}

// TaskAttemptFailure reports a terminal error from one physical attempt.
type TaskAttemptFailure struct {
	JobID       plan.JobID          `json:"job_id"`
	StageID     plan.StageID        `json:"stage_id"`
	Attempt     TaskAttemptIdentity `json:"attempt"`
	PartitionID plan.PartitionID    `json:"partition_id"`
	WorkerID    plan.WorkerID       `json:"worker_id"`
	Error       string              `json:"error"`
}
