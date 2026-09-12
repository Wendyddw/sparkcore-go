package protocol

import (
	"encoding/json"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

const (
	TaskSuccessPath = "/v1/tasks/success"
	TaskFailurePath = "/v1/tasks/failure"
)

// TaskOutput is the wire representation of a partition's output. Each record
// contains one JSON value, preserving its structure and numeric precision.
// Transport adapters marshal scheduler records into RawMessage values; they can
// carry those values through DAG merging without decoding into executor types.
// Count outputs use nil Records; an empty Collect output uses an empty slice.
type TaskOutput struct {
	Records []json.RawMessage `json:"records"`
	Count   int64             `json:"count"`
}

// TaskSuccessRequest reports the output of one physical attempt.
type TaskSuccessRequest struct {
	JobID       plan.JobID                    `json:"job_id"`
	StageID     plan.StageID                  `json:"stage_id"`
	Attempt     scheduler.TaskAttemptIdentity `json:"attempt"`
	PartitionID plan.PartitionID              `json:"partition_id"`
	WorkerID    plan.WorkerID                 `json:"worker_id"`
	Output      TaskOutput                    `json:"output"`
}

// TaskFailureRequest reports a terminal attempt failure as a transport-safe string.
type TaskFailureRequest struct {
	JobID       plan.JobID                    `json:"job_id"`
	StageID     plan.StageID                  `json:"stage_id"`
	Attempt     scheduler.TaskAttemptIdentity `json:"attempt"`
	PartitionID plan.PartitionID              `json:"partition_id"`
	WorkerID    plan.WorkerID                 `json:"worker_id"`
	Error       string                        `json:"error"`
}

// TaskReportResponse acknowledges safe handling of a success or failure report.
// Acknowledged is true for both newly accepted and duplicate/obsolete reports;
// it does not indicate whether the report changed scheduler state. Rejected
// requests receive ErrorResponse with an unsuccessful HTTP status instead.
type TaskReportResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
