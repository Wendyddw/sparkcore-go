package scheduler

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func (s *DAGScheduler) handleTaskFailed(report TaskAttemptFailure) {
	job := s.pendingTask(report.JobID, report.StageID, report.Attempt, report.PartitionID)
	if job == nil {
		return
	}
	if err := job.ctx.Err(); err != nil {
		s.failJob(report.JobID, err)
		return
	}
	if report.Kind == FailureShuffleInput {
		if err := s.recoverShuffle(job, report); err != nil {
			s.failJob(report.JobID, err)
		}
		return
	}
	s.failJob(report.JobID, fmt.Errorf("stage %d task %d partition %d: %s", report.StageID, report.Attempt.TaskID, report.PartitionID, report.Error))
}

// recoverShuffle runs on the DAG event loop. No filesystem work happens here:
// invalidate accepted metadata and let fresh map tasks publish new attempt paths.
func (s *DAGScheduler) recoverShuffle(job *jobState, report TaskAttemptFailure) error {
	child := job.stages[report.StageID]
	if child.stage.Kind != StageResult || len(child.stage.ParentIDs) != 1 {
		return fmt.Errorf("stage %d has no recoverable shuffle parent", report.StageID)
	}
	parent := job.stages[child.stage.ParentIDs[0]]
	if parent == nil || parent.stage.Kind != StageShuffleMap || parent.status != stageSucceeded {
		return fmt.Errorf("stage %d has no completed shuffle parent", report.StageID)
	}
	if err := ValidateFailure(report.Kind, report.ShuffleInput); err != nil {
		return fmt.Errorf("invalid shuffle failure: %w", err)
	}
	ref := report.ShuffleInput
	accepted := parent.outputs[ref.Attempt.MapPartitionID].ShuffleOutput
	if ref.PartitionID != report.PartitionID || accepted == nil || accepted.Attempt != ref.Attempt {
		return fmt.Errorf("shuffle failure does not reference a current accepted input for stage %d", report.StageID)
	}
	for _, stage := range []*stageState{parent, child} {
		if stage.attempts >= s.maxStageAttempts {
			return fmt.Errorf("stage %d exhausted its %d stage attempts after shuffle input failure: %s", stage.stage.ID, s.maxStageAttempts, report.Error)
		}
	}
	for _, stage := range []*stageState{parent, child} {
		stage.cancel()
		stage.cancel = nil
		stage.status = stagePending
		stage.remaining = len(stage.tasks)
		stage.outputs = make(map[plan.PartitionID]TaskOutput)
	}
	s.logger.Warn("shuffle_recovery_started", "job_id", report.JobID,
		"map_stage_id", parent.stage.ID, "result_stage_id", child.stage.ID,
		"failed_stage_attempt_id", report.Attempt.StageAttemptID, "error", report.Error)
	return s.startReadyStages(report.JobID, job)
}
