package scheduler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestPlannerBuildsSingleNarrowResultStage(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(4))
	mapped := addPlannerNode(t, graph, plannerNarrowNode("Map", plan.OpMap, "map", source, 4))
	filtered := addPlannerNode(t, graph, plannerNarrowNode("Filter", plan.OpFilter, "filter", mapped, 4))
	registry := &recordingFunctionLookup{exists: true}

	stagePlan, err := NewPlanner(registry).Plan(graph, ActionSpec{Kind: ActionCount, TargetRDD: filtered})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(stagePlan.Stages) != 1 {
		t.Fatalf("stage count = %d, want 1", len(stagePlan.Stages))
	}

	stage := stagePlan.Stages[0]
	if stage.ID != 0 || stage.Kind != StageResult {
		t.Errorf("stage identity = (%d, %q), want (0, %q)", stage.ID, stage.Kind, StageResult)
	}
	if stage.NumPartitions != 4 {
		t.Errorf("stage partitions = %d, want 4", stage.NumPartitions)
	}
	if len(stage.ParentIDs) != 0 || stage.ShuffleWrite != nil {
		t.Errorf("narrow stage parents/write = (%#v, %#v), want none", stage.ParentIDs, stage.ShuffleWrite)
	}
	if stage.FinalAction == nil || stage.FinalAction.Kind != ActionCount || stage.FinalAction.TargetRDD != filtered {
		t.Errorf("final action = %#v, want Count on RDD %d", stage.FinalAction, filtered)
	}
	assertRDDOperationKinds(t, stage.Operations, plan.OpSource, plan.OpMap, plan.OpFilter)
}

func TestPlannerBuildsTwoStagesAtShuffleBoundary(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(4))
	paired := addPlannerNode(t, graph, plannerNarrowNode("MapToPair", plan.OpMapToPair, "pair", source, 4))
	partitioner := plan.HashPartitioner(2)
	reduced := addPlannerNode(t, graph, plannerShuffleNode("ReduceByKey", "sum", paired, partitioner, 9))
	registry := &recordingFunctionLookup{exists: true}

	stagePlan, err := NewPlanner(registry).Plan(graph, ActionSpec{Kind: ActionCollect, TargetRDD: reduced})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(stagePlan.Stages) != 2 {
		t.Fatalf("stage count = %d, want 2", len(stagePlan.Stages))
	}

	shuffleStage := stagePlan.Stages[0]
	if shuffleStage.ID != 0 || shuffleStage.Kind != StageShuffleMap {
		t.Errorf("first stage = (%d, %q), want (0, %q)", shuffleStage.ID, shuffleStage.Kind, StageShuffleMap)
	}
	if shuffleStage.NumPartitions != 4 {
		t.Errorf("shuffle-map partitions = %d, want 4", shuffleStage.NumPartitions)
	}
	if shuffleStage.ShuffleWrite == nil || shuffleStage.ShuffleWrite.ShuffleID != 9 {
		t.Errorf("shuffle write = %#v, want shuffle 9", shuffleStage.ShuffleWrite)
	}
	if shuffleStage.FinalAction != nil {
		t.Errorf("shuffle-map final action = %#v, want nil", shuffleStage.FinalAction)
	}
	assertRDDOperationKinds(t, shuffleStage.Operations, plan.OpSource, plan.OpMapToPair)

	resultStage := stagePlan.Stages[1]
	if resultStage.ID != 1 || resultStage.Kind != StageResult {
		t.Errorf("second stage = (%d, %q), want (1, %q)", resultStage.ID, resultStage.Kind, StageResult)
	}
	if !reflect.DeepEqual(resultStage.ParentIDs, []plan.StageID{0}) {
		t.Errorf("result parents = %#v, want [0]", resultStage.ParentIDs)
	}
	if resultStage.NumPartitions != 2 {
		t.Errorf("result partitions = %d, want 2", resultStage.NumPartitions)
	}
	if resultStage.FinalAction == nil || resultStage.FinalAction.Kind != ActionCollect {
		t.Errorf("result final action = %#v, want Collect", resultStage.FinalAction)
	}
	if len(resultStage.Operations) != 2 {
		t.Fatalf("result operation count = %d, want 2", len(resultStage.Operations))
	}
	read := resultStage.Operations[0]
	if read.Kind != StageOperationShuffleRead || read.ShuffleRead == nil || read.ShuffleRead.ShuffleID != 9 {
		t.Errorf("first result operation = %#v, want ShuffleRead 9", read)
	}
	assertRDDOperationKinds(t, resultStage.Operations[1:], plan.OpReduceByKey)
}

func TestPlannerValidatesFunctionsBeforeCreatingStages(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(1))
	target := addPlannerNode(t, graph, plannerNarrowNode("Map", plan.OpMap, "missing", source, 1))

	stagePlan, err := NewPlanner(&recordingFunctionLookup{}).Plan(
		graph,
		ActionSpec{Kind: ActionCount, TargetRDD: target},
	)
	if err == nil {
		t.Fatal("Plan() error = nil, want unknown-function error")
	}
	if !strings.Contains(err.Error(), "missing") || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("Plan() error = %q, want unknown function details", err)
	}
	if len(stagePlan.Stages) != 0 {
		t.Fatalf("stage count = %d, want 0", len(stagePlan.Stages))
	}
}

func TestPlannerProducesDeterministicOutput(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(4))
	paired := addPlannerNode(t, graph, plannerNarrowNode("MapToPair", plan.OpMapToPair, "pair", source, 4))
	target := addPlannerNode(t, graph, plannerShuffleNode("ReduceByKey", "sum", paired, plan.HashPartitioner(2), 4))
	registry := &recordingFunctionLookup{exists: true}
	planner := NewPlanner(registry)
	action := ActionSpec{Kind: ActionCollect, TargetRDD: target}

	first, err := planner.Plan(graph, action)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}
	second, err := planner.Plan(graph, action)
	if err != nil {
		t.Fatalf("second Plan() error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated plans differ:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func TestStageBuilderReusesShuffleStage(t *testing.T) {
	graph := plan.NewRDDGraph()
	parent := addPlannerNode(t, graph, plannerSourceNode(4))
	partitioner := plan.HashPartitioner(2)
	dependency := plan.Dependency{
		Kind:     plan.DependencyShuffle,
		ParentID: parent,
		Shuffle: &plan.ShuffleDependencySpec{
			ShuffleID:    12,
			Partitioner:  partitioner,
			AggregatorID: "sum",
		},
	}
	builder := stageBuilder{graph: graph, shuffleStages: make(map[plan.ShuffleID]plan.StageID)}

	first, err := builder.buildShuffleStage(dependency)
	if err != nil {
		t.Fatalf("first buildShuffleStage() error = %v", err)
	}
	second, err := builder.buildShuffleStage(dependency)
	if err != nil {
		t.Fatalf("second buildShuffleStage() error = %v", err)
	}
	if first != second {
		t.Errorf("shuffle stage IDs = %d and %d, want reuse", first, second)
	}
	if len(builder.stages) != 1 {
		t.Fatalf("stage count = %d, want 1", len(builder.stages))
	}
}

func plannerSourceNode(partitions int) plan.RDDNode {
	return plan.RDDNode{
		Name:          "TextFile",
		Operator:      plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"},
		NumPartitions: partitions,
	}
}

func plannerNarrowNode(
	name string,
	kind plan.OperatorKind,
	functionID string,
	parent plan.RDDID,
	partitions int,
) plan.RDDNode {
	return plan.RDDNode{
		Name:     name,
		Operator: plan.OperatorSpec{Kind: kind, FunctionID: functionID},
		Dependencies: []plan.Dependency{{
			Kind:     plan.DependencyNarrow,
			ParentID: parent,
			Narrow:   &plan.NarrowDependencySpec{Mapping: plan.NarrowOneToOne},
		}},
		NumPartitions: partitions,
	}
}

func plannerShuffleNode(
	name string,
	functionID string,
	parent plan.RDDID,
	partitioner plan.PartitionerSpec,
	shuffleID plan.ShuffleID,
) plan.RDDNode {
	return plan.RDDNode{
		Name:          name,
		Operator:      plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: functionID},
		NumPartitions: partitioner.NumPartitions,
		Partitioner:   &partitioner,
		Dependencies: []plan.Dependency{{
			Kind:     plan.DependencyShuffle,
			ParentID: parent,
			Shuffle: &plan.ShuffleDependencySpec{
				ShuffleID:      shuffleID,
				Partitioner:    partitioner,
				AggregatorID:   functionID,
				MapSideCombine: true,
			},
		}},
	}
}

func addPlannerNode(t *testing.T, graph *plan.RDDGraph, node plan.RDDNode) plan.RDDID {
	t.Helper()
	id, err := graph.AddNode(node)
	if err != nil {
		t.Fatalf("AddNode(%s) error = %v", node.Name, err)
	}
	return id
}

func assertRDDOperationKinds(t *testing.T, operations []StageOperation, want ...plan.OperatorKind) {
	t.Helper()
	if len(operations) != len(want) {
		t.Fatalf("operation count = %d, want %d", len(operations), len(want))
	}
	for i, kind := range want {
		if operations[i].Kind != StageOperationRDD || operations[i].RDD == nil {
			t.Fatalf("operation %d = %#v, want RDD operation", i, operations[i])
		}
		if operations[i].RDD.Operator.Kind != kind {
			t.Errorf("operation %d kind = %q, want %q", i, operations[i].RDD.Operator.Kind, kind)
		}
	}
}
