package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func (r *LocalRunner) runShuffleMap(ctx context.Context, execution scheduler.TaskExecution) (output scheduler.TaskOutput, err error) {
	task := execution.Task
	write := task.ShuffleWrite
	if write == nil || task.FinalAction != nil {
		return output, permanentErrorf("shuffle-map task requires a shuffle write and no final action")
	}
	if task.NumPartitions <= 0 || task.PartitionID < 0 || int(task.PartitionID) >= task.NumPartitions {
		return output, permanentErrorf("invalid map partition %d of %d", task.PartitionID, task.NumPartitions)
	}
	if write.Partitioner.Kind != plan.PartitionerHash || write.Partitioner.NumPartitions <= 0 {
		return output, permanentErrorf("shuffle write requires a hash partitioner with positive partitions")
	}
	if r.shuffleStore == nil {
		return output, permanentErrorf("shuffle store is not configured")
	}
	var reduce ReduceFunc
	if write.MapSideCombine {
		reduce, err = r.registry.Reduce(write.AggregatorID)
		if err != nil {
			return output, err
		}
	}
	identity := shuffle.AttemptIdentity{
		RunID: execution.RunID, JobID: execution.JobID, ShuffleID: write.ShuffleID,
		StageID: task.StageID, StageAttemptID: execution.Attempt.StageAttemptID,
		TaskID: task.ID, TaskAttemptID: execution.Attempt.ID, MapPartitionID: task.PartitionID,
	}
	writer, err := r.shuffleStore.Begin(ctx, identity, task.NumPartitions, write.Partitioner.NumPartitions)
	if err != nil {
		return output, err
	}
	defer func() {
		if abortErr := writer.Abort(); abortErr != nil && !errors.Is(err, abortErr) {
			err = errors.Join(err, abortErr)
		}
		if err != nil {
			output = scheduler.TaskOutput{}
		}
	}()

	iterator, err := buildTaskIterator(ctx, execution, r.registry, r.sources, r.shuffleStore)
	if err != nil {
		return output, err
	}
	err = writeShuffleRecords(ctx, iterator, writer, reduce)
	if err = errors.Join(err, closeIterator(iterator)); err != nil {
		return output, err
	}
	if err := context.Cause(ctx); err != nil {
		return output, err
	}
	// Publication makes complete files visible; a later success report requests acceptance.
	published, err := writer.Publish()
	if err != nil {
		return output, err
	}
	if err := context.Cause(ctx); err != nil {
		return output, err
	}
	return scheduler.TaskOutput{ShuffleOutput: &published}, nil
}

func writeShuffleRecords(ctx context.Context, iterator Iterator, writer shuffle.AttemptWriter, reduce ReduceFunc) error {
	// Combining is partition-local and in memory; spilling is outside this implementation.
	var combined map[string]Record
	if reduce != nil {
		combined = make(map[string]Record)
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		record, ok, err := iterator.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		pair, ok := record.(KeyValue)
		if !ok {
			return permanentErrorf("shuffle write expected KeyValue, got %T", record)
		}
		if reduce == nil {
			if err := writer.Write(shuffle.Record{Key: pair.Key, Value: pair.Value}); err != nil {
				return err
			}
			continue
		}
		value := pair.Value
		if previous, exists := combined[pair.Key]; exists {
			value, err = reduce(previous, value)
			if err != nil {
				return fmt.Errorf("combine key %q: %w", pair.Key, err)
			}
		}
		combined[pair.Key] = value
	}
	keys := make([]string, 0, len(combined))
	for key := range combined {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := writer.Write(shuffle.Record{Key: key, Value: combined[key]}); err != nil {
			return err
		}
	}
	return nil
}
