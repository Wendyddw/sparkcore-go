package scheduler

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// GenerateTasks creates one logical task per stage output partition.
// Tasks are ordered by stage, then partition, with deterministic IDs.
func GenerateTasks(stagePlan StagePlan) ([]Task, error) {
	taskCount := 0
	for _, stage := range stagePlan.Stages {
		if stage.NumPartitions <= 0 {
			return nil, fmt.Errorf(
				"generate tasks for stage %d: invalid partition count %d",
				stage.ID,
				stage.NumPartitions,
			)
		}
		taskCount += stage.NumPartitions
	}

	tasks := make([]Task, 0, taskCount)
	for _, stage := range stagePlan.Stages {
		for partition := 0; partition < stage.NumPartitions; partition++ {
			tasks = append(tasks, Task{
				ID:           plan.TaskID(len(tasks)),
				StageID:      stage.ID,
				StageKind:    stage.Kind,
				PartitionID:  plan.PartitionID(partition),
				Operations:   cloneStageOperations(stage.Operations),
				ShuffleWrite: cloneShuffleWrite(stage.ShuffleWrite),
				FinalAction:  cloneAction(stage.FinalAction),
			})
		}
	}
	return tasks, nil
}

func cloneStageOperations(operations []StageOperation) []StageOperation {
	if operations == nil {
		return nil
	}
	clones := make([]StageOperation, len(operations))
	for i, operation := range operations {
		clones[i] = operation
		if operation.RDD != nil {
			rdd := *operation.RDD
			clones[i].RDD = &rdd
		}
		if operation.ShuffleRead != nil {
			shuffleRead := *operation.ShuffleRead
			clones[i].ShuffleRead = &shuffleRead
		}
	}
	return clones
}

func cloneShuffleWrite(shuffleWrite *ShuffleWriteSpec) *ShuffleWriteSpec {
	if shuffleWrite == nil {
		return nil
	}
	clone := *shuffleWrite
	return &clone
}

func cloneAction(action *ActionSpec) *ActionSpec {
	if action == nil {
		return nil
	}
	clone := *action
	return &clone
}
