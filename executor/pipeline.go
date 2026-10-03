package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func buildTaskIterator(
	ctx context.Context,
	execution scheduler.TaskExecution,
	registry *FunctionRegistry,
	sources SourceReader,
	store shuffle.Store,
) (result Iterator, err error) {
	task := execution.Task
	var iterator Iterator
	defer func() {
		if err != nil {
			err = errors.Join(err, closeIterator(iterator))
		}
	}()
	for _, operation := range task.Operations {
		if operation.Kind == scheduler.StageOperationShuffleRead {
			iterator = &shuffleIterator{store: store, inputs: execution.ShuffleInputs.Clone(), partition: task.PartitionID}
			continue
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
		case plan.OpReduceByKey:
			if iterator == nil {
				return nil, fmt.Errorf("reduce-by-key has no input iterator")
			}
			fn, err := registry.Reduce(operator.FunctionID)
			if err != nil {
				return nil, err
			}
			iterator = &reduceIterator{input: iterator, fn: fn}
		default:
			return nil, fmt.Errorf("operator %q is not supported by the executor", operator.Kind)
		}
	}
	if iterator == nil {
		return nil, fmt.Errorf("task has no operations")
	}
	return iterator, nil
}
