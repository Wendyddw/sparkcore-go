package executor

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func buildTaskIterator(
	ctx context.Context,
	task scheduler.Task,
	registry *FunctionRegistry,
	sources SourceReader,
) (Iterator, error) {
	var iterator Iterator
	for _, operation := range task.Operations {
		if operation.Kind == scheduler.StageOperationShuffleRead {
			return nil, fmt.Errorf("shuffle read is not implemented in Week 1")
		}
		if operation.Kind != scheduler.StageOperationRDD || operation.RDD == nil {
			return nil, fmt.Errorf("invalid stage operation %q", operation.Kind)
		}

		operator := operation.RDD.Operator
		switch operator.Kind {
		case plan.OpSource:
			if iterator != nil {
				return nil, fmt.Errorf("source must be the first pipeline operation")
			}
			var err error
			iterator, err = sources.Open(ctx, operator.SourcePath, task.PartitionID, task.NumPartitions)
			if err != nil {
				return nil, err
			}
			if iterator == nil {
				return nil, fmt.Errorf("source reader returned a nil iterator")
			}
		case plan.OpMap:
			if iterator == nil {
				return nil, fmt.Errorf("operator %q has no input iterator", operator.Kind)
			}
			fn, err := registry.Map(operator.FunctionID)
			if err != nil {
				return nil, err
			}
			iterator = &mapIterator{input: iterator, fn: fn}
		case plan.OpFilter:
			if iterator == nil {
				return nil, fmt.Errorf("operator %q has no input iterator", operator.Kind)
			}
			fn, err := registry.Filter(operator.FunctionID)
			if err != nil {
				return nil, err
			}
			iterator = &filterIterator{input: iterator, fn: fn}
		case plan.OpMapToPair:
			if iterator == nil {
				return nil, fmt.Errorf("operator %q has no input iterator", operator.Kind)
			}
			fn, err := registry.PairMap(operator.FunctionID)
			if err != nil {
				return nil, err
			}
			iterator = &pairMapIterator{input: iterator, fn: fn}
		case plan.OpMapValues:
			if iterator == nil {
				return nil, fmt.Errorf("operator %q has no input iterator", operator.Kind)
			}
			fn, err := registry.ValueMap(operator.FunctionID)
			if err != nil {
				return nil, err
			}
			iterator = &valueMapIterator{input: iterator, fn: fn}
		default:
			return nil, fmt.Errorf("operator %q is not supported by the narrow executor", operator.Kind)
		}
	}
	if iterator == nil {
		return nil, fmt.Errorf("task has no operations")
	}
	return iterator, nil
}
