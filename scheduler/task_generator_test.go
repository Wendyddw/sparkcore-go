package scheduler

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestGenerateTasksCreatesOneTaskPerNarrowStagePartition(t *testing.T) {
	stagePlan := StagePlan{
		TargetRDD: 2,
		Stages: []Stage{{
			ID:            0,
			Kind:          StageResult,
			NumPartitions: 4,
			Operations: []StageOperation{
				rddTaskOperation(0, plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"}),
				rddTaskOperation(1, plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "map"}),
				rddTaskOperation(2, plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "filter"}),
			},
			FinalAction: &ActionSpec{Kind: ActionCount, TargetRDD: 2},
		}},
	}

	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		t.Fatalf("GenerateTasks() error = %v", err)
	}
	if len(tasks) != 4 {
		t.Fatalf("task count = %d, want 4", len(tasks))
	}
	for i, task := range tasks {
		if task.ID != plan.TaskID(i) || task.StageID != 0 || task.PartitionID != plan.PartitionID(i) {
			t.Errorf(
				"task %d identity = (id=%d stage=%d partition=%d), want (%d, 0, %d)",
				i,
				task.ID,
				task.StageID,
				task.PartitionID,
				i,
				i,
			)
		}
		assertTaskRDDKinds(t, task, plan.OpSource, plan.OpMap, plan.OpFilter)
		if task.FinalAction == nil || task.FinalAction.Kind != ActionCount {
			t.Errorf("task %d final action = %#v, want Count", i, task.FinalAction)
		}
	}
}

func TestGenerateTasksUsesEachStagesOutputWidth(t *testing.T) {
	partitioner := plan.HashPartitioner(2)
	stagePlan := StagePlan{
		TargetRDD: 2,
		Stages: []Stage{
			{
				ID:            0,
				Kind:          StageShuffleMap,
				NumPartitions: 4,
				Operations: []StageOperation{
					rddTaskOperation(0, plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"}),
					rddTaskOperation(1, plan.OperatorSpec{Kind: plan.OpMapToPair, FunctionID: "pair"}),
				},
				ShuffleWrite: &ShuffleWriteSpec{ShuffleID: 7, Partitioner: partitioner},
			},
			{
				ID:            1,
				Kind:          StageResult,
				ParentIDs:     []plan.StageID{0},
				NumPartitions: 2,
				Operations: []StageOperation{
					{
						Kind: StageOperationShuffleRead,
						ShuffleRead: &ShuffleReadSpec{
							ShuffleID:   7,
							Partitioner: partitioner,
						},
					},
					rddTaskOperation(2, plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: "sum"}),
				},
				FinalAction: &ActionSpec{Kind: ActionCollect, TargetRDD: 2},
			},
		},
	}

	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		t.Fatalf("GenerateTasks() error = %v", err)
	}
	if len(tasks) != 6 {
		t.Fatalf("task count = %d, want 6", len(tasks))
	}
	for i := 0; i < 4; i++ {
		if tasks[i].StageID != 0 || tasks[i].PartitionID != plan.PartitionID(i) {
			t.Errorf("shuffle-map task %d = %#v, want stage 0 partition %d", i, tasks[i], i)
		}
		if tasks[i].ShuffleWrite == nil || tasks[i].FinalAction != nil {
			t.Errorf("shuffle-map task %d output metadata is incorrect", i)
		}
	}
	for i := 0; i < 2; i++ {
		task := tasks[i+4]
		if task.ID != plan.TaskID(i+4) || task.StageID != 1 || task.PartitionID != plan.PartitionID(i) {
			t.Errorf("result task %d = %#v, want ID %d stage 1 partition %d", i, task, i+4, i)
		}
		if task.FinalAction == nil || task.ShuffleWrite != nil {
			t.Errorf("result task %d output metadata is incorrect", i)
		}
	}
}

func TestGenerateTasksCopiesStageExecutionMetadata(t *testing.T) {
	stagePlan := StagePlan{Stages: []Stage{{
		ID:            0,
		Kind:          StageResult,
		NumPartitions: 2,
		Operations: []StageOperation{
			rddTaskOperation(0, plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "original"}),
		},
		FinalAction: &ActionSpec{Kind: ActionCount, TargetRDD: 0},
	}}}

	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		t.Fatalf("GenerateTasks() error = %v", err)
	}
	tasks[0].Operations[0].RDD.Operator.FunctionID = "changed"
	tasks[0].FinalAction.Kind = ActionCollect

	if got := stagePlan.Stages[0].Operations[0].RDD.Operator.FunctionID; got != "original" {
		t.Errorf("stage function ID = %q, want original", got)
	}
	if got := tasks[1].Operations[0].RDD.Operator.FunctionID; got != "original" {
		t.Errorf("second task function ID = %q, want original", got)
	}
	if stagePlan.Stages[0].FinalAction.Kind != ActionCount || tasks[1].FinalAction.Kind != ActionCount {
		t.Error("changing one task's action changed shared metadata")
	}
}

func TestGenerateTasksRejectsInvalidPartitionCount(t *testing.T) {
	stagePlan := StagePlan{Stages: []Stage{{ID: 3, NumPartitions: 0}}}

	tasks, err := GenerateTasks(stagePlan)
	if err == nil {
		t.Fatal("GenerateTasks() error = nil, want invalid partition count error")
	}
	if tasks != nil {
		t.Fatalf("GenerateTasks() tasks = %#v, want nil", tasks)
	}
}

func rddTaskOperation(id plan.RDDID, operator plan.OperatorSpec) StageOperation {
	return StageOperation{
		Kind: StageOperationRDD,
		RDD:  &RDDOperationSpec{RDDID: id, Operator: operator},
	}
}

func assertTaskRDDKinds(t *testing.T, task Task, want ...plan.OperatorKind) {
	t.Helper()
	if len(task.Operations) != len(want) {
		t.Fatalf("task %d operation count = %d, want %d", task.ID, len(task.Operations), len(want))
	}
	for i, kind := range want {
		operation := task.Operations[i]
		if operation.RDD == nil || operation.RDD.Operator.Kind != kind {
			t.Errorf("task %d operation %d = %#v, want RDD operator %q", task.ID, i, operation, kind)
		}
	}
}
