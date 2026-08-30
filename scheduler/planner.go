package scheduler

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// Planner converts validated RDD lineage into a deterministic stage DAG.
type Planner struct {
	functions FunctionLookup
}

// NewPlanner creates a stage planner backed by the function registry lookup.
func NewPlanner(functions FunctionLookup) *Planner {
	return &Planner{functions: functions}
}

// Plan builds parent-before-child stages for an action without generating tasks.
func (p *Planner) Plan(graph *plan.RDDGraph, action ActionSpec) (StagePlan, error) {
	if p == nil {
		return StagePlan{}, fmt.Errorf("plan action %q for RDD %d: planner is nil", action.Kind, action.TargetRDD)
	}
	if err := validateAction(action); err != nil {
		return StagePlan{}, err
	}
	if err := validateFunctions(graph, action.TargetRDD, p.functions); err != nil {
		return StagePlan{}, fmt.Errorf("plan action %q for RDD %d: %w", action.Kind, action.TargetRDD, err)
	}

	builder := stageBuilder{
		graph:         graph,
		shuffleStages: make(map[plan.ShuffleID]plan.StageID),
	}
	if _, err := builder.buildStage(action.TargetRDD, StageResult, nil, &action); err != nil {
		return StagePlan{}, err
	}

	return StagePlan{
		TargetRDD: action.TargetRDD,
		Stages:    builder.stages,
	}, nil
}

func validateAction(action ActionSpec) error {
	switch action.Kind {
	case ActionCollect, ActionCount:
		return nil
	default:
		return fmt.Errorf("plan action %q for RDD %d: unsupported action", action.Kind, action.TargetRDD)
	}
}

type stageBuilder struct {
	graph         *plan.RDDGraph
	stages        []Stage
	shuffleStages map[plan.ShuffleID]plan.StageID
}

func (b *stageBuilder) buildStage(
	target plan.RDDID,
	kind StageKind,
	shuffleWrite *ShuffleWriteSpec,
	finalAction *ActionSpec,
) (plan.StageID, error) {
	operations, parentIDs, err := b.buildPipeline(target)
	if err != nil {
		return 0, err
	}
	node, _ := b.graph.Node(target)
	id := plan.StageID(len(b.stages))
	b.stages = append(b.stages, Stage{
		ID:            id,
		Kind:          kind,
		ParentIDs:     parentIDs,
		NumPartitions: node.NumPartitions,
		Operations:    operations,
		ShuffleWrite:  shuffleWrite,
		FinalAction:   finalAction,
	})
	return id, nil
}

func (b *stageBuilder) buildPipeline(id plan.RDDID) ([]StageOperation, []plan.StageID, error) {
	node, ok := b.graph.Node(id)
	if !ok {
		return nil, nil, fmt.Errorf("build stage pipeline: RDD %d does not exist", id)
	}

	operation := StageOperation{
		Kind: StageOperationRDD,
		RDD: &RDDOperationSpec{
			RDDID:    node.ID,
			Operator: node.Operator,
		},
	}
	if len(node.Dependencies) == 0 {
		return []StageOperation{operation}, nil, nil
	}

	dependency := node.Dependencies[0]
	switch dependency.Kind {
	case plan.DependencyNarrow:
		operations, parentIDs, err := b.buildPipeline(dependency.ParentID)
		if err != nil {
			return nil, nil, err
		}
		return append(operations, operation), parentIDs, nil
	case plan.DependencyShuffle:
		parentStageID, err := b.buildShuffleStage(dependency)
		if err != nil {
			return nil, nil, err
		}
		shuffleRead := StageOperation{
			Kind: StageOperationShuffleRead,
			ShuffleRead: &ShuffleReadSpec{
				ShuffleID:   dependency.Shuffle.ShuffleID,
				Partitioner: dependency.Shuffle.Partitioner,
			},
		}
		return []StageOperation{shuffleRead, operation}, []plan.StageID{parentStageID}, nil
	default:
		return nil, nil, fmt.Errorf(
			"build stage pipeline for RDD %d (%s): unsupported dependency kind %q",
			node.ID,
			node.Name,
			dependency.Kind,
		)
	}
}

func (b *stageBuilder) buildShuffleStage(dependency plan.Dependency) (plan.StageID, error) {
	shuffle := dependency.Shuffle
	if id, ok := b.shuffleStages[shuffle.ShuffleID]; ok {
		return id, nil
	}

	shuffleWrite := &ShuffleWriteSpec{
		ShuffleID:      shuffle.ShuffleID,
		Partitioner:    shuffle.Partitioner,
		AggregatorID:   shuffle.AggregatorID,
		MapSideCombine: shuffle.MapSideCombine,
	}
	id, err := b.buildStage(dependency.ParentID, StageShuffleMap, shuffleWrite, nil)
	if err != nil {
		return 0, err
	}
	b.shuffleStages[shuffle.ShuffleID] = id
	return id, nil
}
