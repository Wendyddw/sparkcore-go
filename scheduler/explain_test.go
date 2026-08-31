package scheduler

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestExplainStagePlanRendersTwoStageDAG(t *testing.T) {
	partitioner := plan.HashPartitioner(2)
	stagePlan := StagePlan{
		TargetRDD: 2,
		Stages: []Stage{
			{
				ID:            0,
				Kind:          StageShuffleMap,
				NumPartitions: 4,
				Operations: []StageOperation{
					rddStageOperation(0, plan.OpSource),
					rddStageOperation(1, plan.OpMapToPair),
				},
				ShuffleWrite: &ShuffleWriteSpec{
					ShuffleID:      9,
					Partitioner:    partitioner,
					AggregatorID:   "sum",
					MapSideCombine: true,
				},
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
							ShuffleID:   9,
							Partitioner: partitioner,
						},
					},
					rddStageOperation(2, plan.OpReduceByKey),
				},
				FinalAction: &ActionSpec{Kind: ActionCollect, TargetRDD: 2},
			},
		},
	}

	got := ExplainStagePlan(stagePlan)
	want := "== Stage DAG ==\n" +
		"Target RDD 2\n" +
		"Stage 0 kind=shuffle_map partitions=4 parents=[] tasks=4\n" +
		"  Pipeline: RDD 0 source -> RDD 1 map_to_pair\n" +
		"  ShuffleWrite: shuffle=9 partitioner=hash[2] aggregator=\"sum\" map_side_combine=true\n" +
		"Stage 1 kind=result partitions=2 parents=[0] tasks=2\n" +
		"  Pipeline: ShuffleRead 9 hash[2] -> RDD 2 reduce_by_key\n" +
		"  Action: collect target=RDD 2\n"
	if got != want {
		t.Fatalf("ExplainStagePlan() =\n%s\nwant:\n%s", got, want)
	}
}

func TestExplainStagePlanIsDeterministic(t *testing.T) {
	stagePlan := StagePlan{
		TargetRDD: 1,
		Stages: []Stage{{
			ID:            0,
			Kind:          StageResult,
			NumPartitions: 4,
			Operations: []StageOperation{
				rddStageOperation(0, plan.OpSource),
				rddStageOperation(1, plan.OpFilter),
			},
			FinalAction: &ActionSpec{Kind: ActionCount, TargetRDD: 1},
		}},
	}

	first := ExplainStagePlan(stagePlan)
	second := ExplainStagePlan(stagePlan)
	if first != second {
		t.Fatalf("repeated explanations differ:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func rddStageOperation(id plan.RDDID, kind plan.OperatorKind) StageOperation {
	return StageOperation{
		Kind: StageOperationRDD,
		RDD:  &RDDOperationSpec{RDDID: id, Operator: plan.OperatorSpec{Kind: kind}},
	}
}
