package scheduler

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestStageOperationsRepresentSourceToOutputOrder(t *testing.T) {
	operations := []StageOperation{
		{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    0,
				Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"},
			},
		},
		{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    1,
				Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "parse-event"},
			},
		},
		{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    2,
				Operator: plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "is-relevant"},
			},
		},
	}
	stage := Stage{Operations: operations}

	wantKinds := []plan.OperatorKind{plan.OpSource, plan.OpMap, plan.OpFilter}
	for i, wantKind := range wantKinds {
		operation := stage.Operations[i]
		if operation.Kind != StageOperationRDD {
			t.Errorf("operation %d kind = %q, want %q", i, operation.Kind, StageOperationRDD)
		}
		if operation.RDD == nil || operation.RDD.Operator.Kind != wantKind {
			t.Errorf("operation %d RDD = %#v, want operator %q", i, operation.RDD, wantKind)
		}
		if operation.ShuffleRead != nil {
			t.Errorf("operation %d shuffle read = %#v, want nil", i, operation.ShuffleRead)
		}
	}
}

func TestStageOperationsRepresentShuffleReadBeforeReduce(t *testing.T) {
	partitioner := plan.HashPartitioner(2)
	stage := Stage{Operations: []StageOperation{
		{
			Kind: StageOperationShuffleRead,
			ShuffleRead: &ShuffleReadSpec{
				ShuffleID:   3,
				Partitioner: partitioner,
			},
		},
		{
			Kind: StageOperationRDD,
			RDD: &RDDOperationSpec{
				RDDID:    4,
				Operator: plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: "sum"},
			},
		},
	}}

	if stage.Operations[0].Kind != StageOperationShuffleRead || stage.Operations[0].ShuffleRead == nil {
		t.Fatalf("first operation = %#v, want shuffle read", stage.Operations[0])
	}
	if stage.Operations[0].RDD != nil {
		t.Errorf("shuffle-read RDD spec = %#v, want nil", stage.Operations[0].RDD)
	}
	if stage.Operations[1].Kind != StageOperationRDD || stage.Operations[1].RDD == nil {
		t.Fatalf("second operation = %#v, want RDD operation", stage.Operations[1])
	}
	if stage.Operations[1].RDD.Operator.Kind != plan.OpReduceByKey {
		t.Errorf("second operator = %q, want %q", stage.Operations[1].RDD.Operator.Kind, plan.OpReduceByKey)
	}
}

func TestStageOperationKindsAreDistinct(t *testing.T) {
	if StageOperationRDD == StageOperationShuffleRead {
		t.Fatalf("stage operation kinds are equal: %q", StageOperationRDD)
	}
}
