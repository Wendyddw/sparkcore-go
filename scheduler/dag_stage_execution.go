package scheduler

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

type stageExecutionStatus uint8

const (
	stagePending stageExecutionStatus = iota
	stageRunning
	stageSucceeded
)

type stageState struct {
	stage     Stage
	attemptID plan.StageAttemptID
	status    stageExecutionStatus
	tasks     []Task
	remaining int
	outputs   map[plan.PartitionID]TaskOutput
}

type jobState struct {
	action        ActionSpec
	runID         string
	stages        map[plan.StageID]*stageState
	order         []plan.StageID
	resultStageID plan.StageID
	ctx           context.Context
	response      chan<- jobCompletion
	cancel        context.CancelFunc
}

// Only narrow jobs and one shuffle followed by a result stage are executable.
func validateExecutionPlan(p StagePlan) error {
	if len(p.Stages) == 1 {
		s := p.Stages[0]
		if s.Kind == StageResult && len(s.ParentIDs) == 0 && s.ShuffleWrite == nil {
			return nil
		}
	}
	if len(p.Stages) == 2 {
		parent, child := p.Stages[0], p.Stages[1]
		if parent.Kind == StageShuffleMap && len(parent.ParentIDs) == 0 && parent.ShuffleWrite != nil && child.Kind == StageResult && len(child.ParentIDs) == 1 && child.ParentIDs[0] == parent.ID && len(child.Operations) > 0 {
			read := child.Operations[0].ShuffleRead
			if read != nil && read.ShuffleID == parent.ShuffleWrite.ShuffleID {
				return nil
			}
		}
	}
	return fmt.Errorf("only narrow or single-shuffle execution is supported")
}

// The event loop alone advances readiness; all execution and storage I/O stay outside it.
func (s *DAGScheduler) startReadyStages(jobID plan.JobID, job *jobState) error {
	if err := job.ctx.Err(); err != nil {
		return err
	}
	for _, id := range job.order {
		stage := job.stages[id]
		if stage.status != stagePending {
			continue
		}
		ready := true
		for _, parent := range stage.stage.ParentIDs {
			if job.stages[parent].status != stageSucceeded {
				ready = false
			}
		}
		if !ready {
			continue
		}
		set := TaskSet{JobID: jobID, StageID: id, StageAttemptID: s.nextStageAttemptID}
		if len(stage.stage.ParentIDs) != 0 {
			parent := job.stages[stage.stage.ParentIDs[0]]
			inputs := &shuffle.InputSnapshot{RunID: job.runID, JobID: jobID, ShuffleID: parent.stage.ShuffleWrite.ShuffleID,
				StageID: parent.stage.ID, StageAttemptID: parent.attemptID, NumMapPartitions: len(parent.tasks), NumReducePartitions: stage.stage.NumPartitions,
				Outputs: make([]shuffle.MapOutput, len(parent.tasks))}
			for partition := range parent.tasks {
				inputs.Outputs[partition] = *parent.outputs[plan.PartitionID(partition)].Clone().ShuffleOutput
			}
			if err := inputs.Validate(); err != nil {
				return fmt.Errorf("build stage %d inputs: %w", id, err)
			}
			set.ShuffleInputs = inputs
		}
		for _, task := range stage.tasks {
			set.Tasks = append(set.Tasks, cloneTask(task))
		}
		s.nextStageAttemptID++
		stage.attemptID, stage.status = set.StageAttemptID, stageRunning
		s.logger.Info("stage_started", "job_id", jobID, "stage_id", id, "stage_attempt_id", stage.attemptID,
			"partition_count", len(stage.tasks), "action", job.action.Kind)
		s.workers.Add(1)
		go s.scheduleTaskSet(job.ctx, set)
	}
	return nil
}
