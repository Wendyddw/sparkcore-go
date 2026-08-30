package scheduler

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestTaskIdentityIsSeparateFromAttemptIdentity(t *testing.T) {
	task := Task{
		ID:          8,
		StageID:     2,
		StageKind:   StageResult,
		PartitionID: 3,
		Operations: []StageOperation{{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    5,
				Operator: plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "filter"},
			},
		}},
		FinalAction: &ActionSpec{Kind: ActionCount, TargetRDD: 5},
	}
	firstAttempt := TaskAttemptIdentity{
		ID:             20,
		TaskID:         task.ID,
		StageAttemptID: 4,
	}
	secondAttempt := TaskAttemptIdentity{
		ID:             21,
		TaskID:         task.ID,
		StageAttemptID: 4,
	}

	if firstAttempt.TaskID != task.ID || secondAttempt.TaskID != task.ID {
		t.Fatalf(
			"attempt task IDs = [%d, %d], want logical task %d",
			firstAttempt.TaskID,
			secondAttempt.TaskID,
			task.ID,
		)
	}
	if firstAttempt.ID == secondAttempt.ID {
		t.Fatalf("attempt IDs are equal: %d", firstAttempt.ID)
	}
	if task.ID != 8 || task.StageID != 2 || task.PartitionID != 3 {
		t.Fatalf("logical task identity changed: %#v", task)
	}
}

func TestTaskCarriesStageExecutionTemplate(t *testing.T) {
	partitioner := plan.HashPartitioner(2)
	task := Task{
		ID:          0,
		StageID:     0,
		StageKind:   StageShuffleMap,
		PartitionID: 1,
		Operations: []StageOperation{{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    0,
				Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"},
			},
		}},
		ShuffleWrite: &ShuffleWriteSpec{
			ShuffleID:   7,
			Partitioner: partitioner,
		},
	}

	if task.StageKind != StageShuffleMap || task.PartitionID != 1 {
		t.Errorf("task stage/partition = (%q, %d), want (%q, 1)", task.StageKind, task.PartitionID, StageShuffleMap)
	}
	if len(task.Operations) != 1 || task.Operations[0].RDD == nil {
		t.Fatalf("task operations = %#v, want one RDD operation", task.Operations)
	}
	if task.ShuffleWrite == nil || task.ShuffleWrite.ShuffleID != 7 {
		t.Errorf("task shuffle write = %#v, want shuffle 7", task.ShuffleWrite)
	}
	if task.FinalAction != nil {
		t.Errorf("shuffle-map final action = %#v, want nil", task.FinalAction)
	}
}
