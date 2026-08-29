package scheduler

import (
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestStageModelRepresentsShuffleAndResultStages(t *testing.T) {
	partitioner := plan.HashPartitioner(2)
	shuffleStage := Stage{
		ID:            0,
		Kind:          StageShuffleMap,
		NumPartitions: 4,
		ShuffleWrite: &ShuffleWriteSpec{
			ShuffleID:      7,
			Partitioner:    partitioner,
			AggregatorID:   "sum-values",
			MapSideCombine: true,
		},
	}
	resultStage := Stage{
		ID:            1,
		Kind:          StageResult,
		ParentIDs:     []plan.StageID{shuffleStage.ID},
		NumPartitions: 2,
		FinalAction: &ActionSpec{
			Kind:      ActionCollect,
			TargetRDD: 5,
		},
	}
	stagePlan := StagePlan{
		TargetRDD: 5,
		Stages:    []Stage{shuffleStage, resultStage},
	}

	if stagePlan.Stages[0].Kind != StageShuffleMap {
		t.Errorf("first stage kind = %q, want %q", stagePlan.Stages[0].Kind, StageShuffleMap)
	}
	if stagePlan.Stages[0].ShuffleWrite == nil {
		t.Fatal("shuffle-map stage has no shuffle write")
	}
	if stagePlan.Stages[0].FinalAction != nil {
		t.Errorf("shuffle-map final action = %#v, want nil", stagePlan.Stages[0].FinalAction)
	}
	if stagePlan.Stages[1].Kind != StageResult {
		t.Errorf("second stage kind = %q, want %q", stagePlan.Stages[1].Kind, StageResult)
	}
	if !reflect.DeepEqual(stagePlan.Stages[1].ParentIDs, []plan.StageID{0}) {
		t.Errorf("result parent IDs = %#v, want [0]", stagePlan.Stages[1].ParentIDs)
	}
	if stagePlan.Stages[1].ShuffleWrite != nil {
		t.Errorf("result shuffle write = %#v, want nil", stagePlan.Stages[1].ShuffleWrite)
	}
	if stagePlan.Stages[1].FinalAction == nil || stagePlan.Stages[1].FinalAction.Kind != ActionCollect {
		t.Errorf("result final action = %#v, want collect", stagePlan.Stages[1].FinalAction)
	}
	if stagePlan.Stages[0].NumPartitions != 4 || stagePlan.Stages[1].NumPartitions != 2 {
		t.Errorf(
			"stage partition counts = [%d, %d], want [4, 2]",
			stagePlan.Stages[0].NumPartitions,
			stagePlan.Stages[1].NumPartitions,
		)
	}
}

func TestStageAndActionKindsAreDistinct(t *testing.T) {
	if StageShuffleMap == StageResult {
		t.Fatalf("stage kinds are equal: %q", StageShuffleMap)
	}
	if ActionCollect == ActionCount {
		t.Fatalf("action kinds are equal: %q", ActionCollect)
	}
}
