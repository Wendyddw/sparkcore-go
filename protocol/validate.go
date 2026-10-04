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
	if err := a.Execution().Validate(); err != nil {
		return err
	}
	task := a.Task
	if a.Attempt.TaskID != task.ID || a.StageID != task.StageID {
		return fmt.Errorf("assignment and task identities do not match")
	}
	if task.NumPartitions <= 0 || task.PartitionID < 0 || int(task.PartitionID) >= task.NumPartitions {
		return fmt.Errorf("partition_id must be within num_partitions")
	}
	mapTask := task.StageKind == scheduler.StageShuffleMap
	switch task.StageKind {
	case scheduler.StageShuffleMap:
		if task.FinalAction != nil || task.ShuffleWrite == nil || a.ShuffleInputs != nil {
			return fmt.Errorf("map assignment requires shuffle write and no action or shuffle inputs")
		}
		w := task.ShuffleWrite
		if w.Partitioner.Kind != plan.PartitionerHash || w.Partitioner.NumPartitions <= 0 || strings.TrimSpace(w.AggregatorID) == "" {
			return fmt.Errorf("shuffle write requires hash partitioning and an aggregator")
		}
	case scheduler.StageResult:
		if task.ShuffleWrite != nil || task.FinalAction == nil || (task.FinalAction.Kind != scheduler.ActionCount && task.FinalAction.Kind != scheduler.ActionCollect) {
			return fmt.Errorf("result assignment requires Count or Collect and no shuffle write")
		}
	default:
		return fmt.Errorf("unsupported stage kind %q", task.StageKind)
	}
	if len(task.Operations) == 0 {
		return fmt.Errorf("assignment requires a pipeline")
	}
	readsShuffle := task.Operations[0].Kind == scheduler.StageOperationShuffleRead
	if readsShuffle {
		op := task.Operations[0]
		if mapTask || op.RDD != nil || op.ShuffleRead == nil || a.ShuffleInputs == nil {
			return fmt.Errorf("invalid shuffle read assignment")
		}
		input, read := a.ShuffleInputs, op.ShuffleRead
		if err := input.Validate(); err != nil {
			return err
		}
		if input.RunID != a.RunID || input.JobID != a.JobID || input.ShuffleID != read.ShuffleID || read.Partitioner.Kind != plan.PartitionerHash || read.Partitioner.NumPartitions != input.NumReducePartitions || task.NumPartitions != input.NumReducePartitions {
			return fmt.Errorf("shuffle inputs do not match assignment")
		}
		if len(task.Operations) < 2 || task.Operations[1].RDD == nil || task.Operations[1].RDD.Operator.Kind != plan.OpReduceByKey {
			return fmt.Errorf("shuffle read requires ReduceByKey")
		}
	} else if a.ShuffleInputs != nil {
		return fmt.Errorf("narrow pipeline must not contain shuffle inputs")
	}

	seen := make(map[plan.RDDID]bool, len(task.Operations))
	for i, operation := range task.Operations {
		if i == 0 && readsShuffle {
			continue
		}
		if operation.Kind != scheduler.StageOperationRDD || operation.RDD == nil || operation.ShuffleRead != nil {
			return fmt.Errorf("operation %d must be an RDD operation", i)
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
		case plan.OpMap, plan.OpFilter, plan.OpMapToPair, plan.OpMapValues, plan.OpReduceByKey:
			if (op.Kind == plan.OpReduceByKey) != (readsShuffle && i == 1) {
				return fmt.Errorf("ReduceByKey must immediately follow shuffle read")
			}
			if strings.TrimSpace(op.FunctionID) == "" || op.SourcePath != "" {
				return fmt.Errorf("operation %d requires a function_id and no source_path", i)
			}
		default:
			return fmt.Errorf("unsupported narrow operator %q", op.Kind)
		}
	}
	if !mapTask && task.FinalAction.TargetRDD != task.Operations[len(task.Operations)-1].RDD.RDDID {
		return fmt.Errorf("final action target does not match pipeline output RDD")
	}
	return nil
}
func (o TaskOutput) Validate() error {
	if o.ShuffleOutput != nil {
		if o.Records != nil || o.Count != 0 {
			return fmt.Errorf("shuffle output must not contain action results")
		}
		return o.ShuffleOutput.Validate()
	}
	if o.Count < 0 {
		return fmt.Errorf("count must not be negative")
	}
	if o.Records != nil && o.Count != 0 {
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
	if err := r.Output.Validate(); err != nil {
		return err
	}
	if r.Output.ShuffleOutput != nil {
		a := r.Output.ShuffleOutput.Attempt
		if a.JobID != r.JobID || a.StageID != r.StageID || a.StageAttemptID != r.Attempt.StageAttemptID || a.TaskID != r.Attempt.TaskID || a.TaskAttemptID != r.Attempt.ID || a.MapPartitionID != r.PartitionID {
			return fmt.Errorf("map output identity differs from report")
		}
	}
	return nil
}
func (r TaskFailureRequest) Validate() error {
	if err := scheduler.ValidateFailure(r.Kind, r.ShuffleInput); err != nil {
		return err
	}
	if r.ShuffleInput != nil && (r.ShuffleInput.Attempt.JobID != r.JobID || r.ShuffleInput.PartitionID != r.PartitionID) {
		return fmt.Errorf("shuffle failure input differs from report job or partition")
	}
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
