// Package protocol defines JSON messages for the coordinator's /v1 HTTP API.
// Messages contain transport data only; handlers adapt them to scheduler calls.
// Numeric IDs are zero-based and must be decoded into their typed fields to
// preserve uint64 precision. Validation and strict decoding are added separately.
package protocol

import (
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

const (
	RegisterWorkerPath = "/v1/workers/register"
	HeartbeatPath      = "/v1/workers/heartbeat"
)

// RegisterWorkerRequest is the POST /v1/workers/register body.
// BaseURL is optional metadata reserved for future coordinator-to-worker calls;
// Week 2 workers receive assignments through heartbeat responses.
type RegisterWorkerRequest struct {
	WorkerID   plan.WorkerID `json:"worker_id"`
	TotalSlots int           `json:"total_slots"`
	BaseURL    string        `json:"base_url,omitempty"`
}

// RegisterWorkerResponse confirms the registered identity and slot capacity.
type RegisterWorkerResponse struct {
	WorkerID   plan.WorkerID `json:"worker_id"`
	TotalSlots int           `json:"total_slots"`
}

// HeartbeatRequest reports status and offers capacity for task assignments.
// Total capacity comes from registration; FreeSlots is the current worker view.
// Send an empty running_attempt_ids array when the worker is idle.
type HeartbeatRequest struct {
	WorkerID          plan.WorkerID        `json:"worker_id"`
	FreeSlots         int                  `json:"free_slots"`
	RunningAttemptIDs []plan.TaskAttemptID `json:"running_attempt_ids"`
}

// HeartbeatResponse returns zero or more assignments. Send an empty assignments
// array when no work is available.
type HeartbeatResponse struct {
	Assignments []TaskAssignment `json:"assignments"`
}

// TaskAssignment carries the identity and full partition pipeline for one attempt.
// The worker receives executable metadata, not scheduler-owned lifecycle state.
// Attempt.TaskID and StageID must match Task.ID and Task.StageID respectively.
type TaskAssignment struct {
	JobID    plan.JobID                    `json:"job_id"`
	StageID  plan.StageID                  `json:"stage_id"`
	WorkerID plan.WorkerID                 `json:"worker_id"`
	Attempt  scheduler.TaskAttemptIdentity `json:"attempt"`
	Task     scheduler.Task                `json:"task"`
}
