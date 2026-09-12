package protocol

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// Validate methods check message values without I/O or scheduler state. Numeric
// IDs are zero-based; DecodeAndValidate checks their presence and representable range.
func (r RegisterWorkerRequest) Validate() error {
	if err := validateWorker(r.WorkerID); err != nil {
		return err
	}
	if r.TotalSlots <= 0 {
		return fmt.Errorf("total_slots must be positive")
	}
	if r.BaseURL != "" {
		u, err := url.Parse(r.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return fmt.Errorf("base_url must be an absolute HTTP(S) URL without credentials, query, or fragment")
		}
	}
	return nil
}
func (r RegisterWorkerResponse) Validate() error {
	return (RegisterWorkerRequest{WorkerID: r.WorkerID, TotalSlots: r.TotalSlots}).Validate()
}
func (r HeartbeatRequest) Validate() error {
	if err := validateWorker(r.WorkerID); err != nil {
		return err
	}
	if r.FreeSlots < 0 {
		return fmt.Errorf("free_slots must not be negative")
	}
	if r.RunningAttemptIDs == nil {
		return fmt.Errorf("running_attempt_ids must be an array")
	}
	seen := make(map[plan.TaskAttemptID]bool, len(r.RunningAttemptIDs))
	for _, id := range r.RunningAttemptIDs {
		if seen[id] {
			return fmt.Errorf("duplicate running attempt %d", id)
		}
		seen[id] = true
	}
	return nil
}

// ValidateCapacity checks an offer against the registered capacity supplied by
// the caller. Worker existence, ownership, and reservations remain scheduler checks.
func (r HeartbeatRequest) ValidateCapacity(totalSlots int) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if totalSlots <= 0 || r.FreeSlots > totalSlots || len(r.RunningAttemptIDs) > totalSlots-r.FreeSlots {
		return fmt.Errorf("heartbeat exceeds registered capacity %d", totalSlots)
	}
	return nil
}
func (r HeartbeatResponse) Validate() error {
	if r.Assignments == nil {
		return fmt.Errorf("assignments must be an array")
	}
	attempts := make(map[plan.TaskAttemptID]bool)
	type logicalKey struct {
		job          plan.JobID
		stage        plan.StageID
		stageAttempt plan.StageAttemptID
		task         plan.TaskID
	}
	tasks := make(map[logicalKey]bool)
	for i, a := range r.Assignments {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("assignment %d: %w", i, err)
		}
		if a.WorkerID != r.Assignments[0].WorkerID {
			return fmt.Errorf("assignments target different workers")
		}
		key := logicalKey{a.JobID, a.StageID, a.Attempt.StageAttemptID, a.Attempt.TaskID}
		if attempts[a.Attempt.ID] || tasks[key] {
			return fmt.Errorf("duplicate assignment for task %d attempt %d", a.Attempt.TaskID, a.Attempt.ID)
		}
		attempts[a.Attempt.ID] = true
		tasks[key] = true
	}
	return nil
}

func (a TaskAssignment) Validate() error {
	if err := validateWorker(a.WorkerID); err != nil {
		return err
	}
	task := a.Task
	if a.Attempt.TaskID != task.ID || a.StageID != task.StageID {
		return fmt.Errorf("assignment and task identities do not match")
	}
	if task.NumPartitions <= 0 || task.PartitionID < 0 || int(task.PartitionID) >= task.NumPartitions {
		return fmt.Errorf("partition_id must be within num_partitions")
	}
	if task.StageKind != scheduler.StageResult || task.ShuffleWrite != nil {
		return fmt.Errorf("only narrow result-stage assignments are supported")
	}
	if task.FinalAction == nil || (task.FinalAction.Kind != scheduler.ActionCount && task.FinalAction.Kind != scheduler.ActionCollect) {
		return fmt.Errorf("assignment requires a Count or Collect final action")
	}
	if len(task.Operations) == 0 {
		return fmt.Errorf("assignment requires a source pipeline")
	}
	seen := make(map[plan.RDDID]bool, len(task.Operations))
	for i, operation := range task.Operations {
		if operation.Kind != scheduler.StageOperationRDD || operation.RDD == nil || operation.ShuffleRead != nil {
			return fmt.Errorf("operation %d must be a narrow RDD operation", i)
		}
		rdd := operation.RDD
		if seen[rdd.RDDID] {
			return fmt.Errorf("duplicate RDD ID %d in pipeline", rdd.RDDID)
		}
		seen[rdd.RDDID] = true
		op := rdd.Operator
		if i == 0 {
			if op.Kind != plan.OpSource || strings.TrimSpace(op.SourcePath) == "" || op.FunctionID != "" {
				return fmt.Errorf("pipeline must start with a source path")
			}
			continue
		}
		switch op.Kind {
		case plan.OpMap, plan.OpFilter, plan.OpMapToPair, plan.OpMapValues:
			if strings.TrimSpace(op.FunctionID) == "" || op.SourcePath != "" {
				return fmt.Errorf("operation %d requires a function_id and no source_path", i)
			}
		default:
			return fmt.Errorf("unsupported narrow operator %q", op.Kind)
		}
	}
	if task.FinalAction.TargetRDD != task.Operations[len(task.Operations)-1].RDD.RDDID {
		return fmt.Errorf("final action target does not match pipeline output RDD")
	}
	return nil
}
func (o TaskOutput) Validate() error {
	if o.Count < 0 {
		return fmt.Errorf("count must not be negative")
	}
	if len(o.Records) > 0 && o.Count != 0 {
		return fmt.Errorf("output cannot contain both records and a nonzero count")
	}
	for i, record := range o.Records {
		if !json.Valid(record) {
			return fmt.Errorf("record %d must contain one JSON value", i)
		}
	}
	return nil
}
func (r TaskSuccessRequest) Validate() error {
	if err := validateReport(r.WorkerID, r.PartitionID); err != nil {
		return err
	}
	return r.Output.Validate()
}
func (r TaskFailureRequest) Validate() error {
	if err := validateReport(r.WorkerID, r.PartitionID); err != nil {
		return err
	}
	if strings.TrimSpace(r.Error) == "" {
		return fmt.Errorf("error message must not be empty")
	}
	return nil
}
func (r TaskReportResponse) Validate() error {
	if !r.Acknowledged {
		return fmt.Errorf("successful report response must acknowledge handling")
	}
	return nil
}
func (r JobResultResponse) Validate() error {
	if err := (TaskOutput{Records: r.Records, Count: r.Count}).Validate(); err != nil {
		return err
	}
	switch r.Action {
	case scheduler.ActionCount:
		if r.Records != nil {
			return fmt.Errorf("Count result must use null records")
		}
	case scheduler.ActionCollect:
		if r.Records == nil || r.Count != 0 {
			return fmt.Errorf("Collect result requires a records array and zero count")
		}
	default:
		return fmt.Errorf("unsupported result action %q", r.Action)
	}
	return nil
}
func (r ErrorResponse) Validate() error {
	if strings.TrimSpace(r.Message) == "" {
		return fmt.Errorf("error response message must not be empty")
	}
	switch r.Code {
	case CodeInvalidRequest, CodeRequestTooLarge, CodeNotFound, CodeMethodNotAllowed, CodeConflict, CodeUnavailable, CodeJobFailed, CodeInternal:
		return nil
	default:
		return fmt.Errorf("unknown error code %q", r.Code)
	}
}
func validateWorker(id plan.WorkerID) error {
	if strings.TrimSpace(string(id)) == "" {
		return fmt.Errorf("worker_id must not be empty")
	}
	return nil
}
func validateReport(worker plan.WorkerID, partition plan.PartitionID) error {
	if err := validateWorker(worker); err != nil {
		return err
	}
	if partition < 0 {
		return fmt.Errorf("partition_id must not be negative")
	}
	return nil
}
