package executor

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func validateShuffleExecution(e scheduler.TaskExecution, store shuffle.Store) error {
	var read *scheduler.ShuffleReadSpec
	for index, operation := range e.Task.Operations {
		if operation.Kind == scheduler.StageOperationShuffleRead {
			if index != 0 || operation.ShuffleRead == nil || operation.RDD != nil {
				return fmt.Errorf("shuffle read must be the first operation with a read specification")
			}
			read = operation.ShuffleRead
		}
		if operation.RDD != nil && operation.RDD.Operator.Kind == plan.OpReduceByKey && (read == nil || index != 1) {
			return fmt.Errorf("reduce-by-key must immediately follow shuffle read")
		}
	}
	if read == nil {
		if e.ShuffleInputs != nil {
			return fmt.Errorf("task without shuffle read must not have shuffle inputs")
		}
		return nil
	}
	task := e.Task
	if task.StageKind != scheduler.StageResult || task.ShuffleWrite != nil || task.FinalAction == nil {
		return fmt.Errorf("shuffle read requires a result task")
	}
	if len(task.Operations) < 2 || task.Operations[1].Kind != scheduler.StageOperationRDD || task.Operations[1].RDD == nil || task.Operations[1].RDD.Operator.Kind != plan.OpReduceByKey {
		return fmt.Errorf("shuffle read must be followed by reduce-by-key")
	}
	if store == nil || e.ShuffleInputs == nil {
		return fmt.Errorf("shuffle read requires a store and input snapshot")
	}
	if err := e.ShuffleInputs.Validate(); err != nil {
		return err
	}
	inputs := e.ShuffleInputs
	if inputs.RunID != e.RunID || inputs.JobID != e.JobID || inputs.ShuffleID != read.ShuffleID {
		return fmt.Errorf("shuffle input does not match task run, job or shuffle")
	}
	if read.Partitioner.Kind != plan.PartitionerHash || read.Partitioner.NumPartitions != inputs.NumReducePartitions || task.NumPartitions != inputs.NumReducePartitions || task.PartitionID < 0 || int(task.PartitionID) >= task.NumPartitions {
		return fmt.Errorf("shuffle input does not match task partitioning")
	}
	return nil
}

// shuffleIterator reads one bucket per map in snapshot order, without aggregation.
type shuffleIterator struct {
	store     shuffle.Store
	inputs    shuffle.InputSnapshot
	partition plan.PartitionID
	next      int
	reader    shuffle.BucketReader
	closed    bool
	err       error
}

func (i *shuffleIterator) Next(ctx context.Context) (Record, bool, error) {
	if i.err != nil {
		return nil, false, i.err
	}
	if i.closed {
		return nil, false, nil
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return i.fail(err)
		}
		if i.next == len(i.inputs.Outputs) {
			i.closed = true
			return nil, false, nil
		}
		if i.reader == nil {
			reader, err := i.store.OpenBucket(ctx, i.inputs.Outputs[i.next], i.partition)
			if err != nil {
				return i.fail(i.inputError(ctx, err))
			}
			i.reader = reader
		}
		record, err := i.reader.Next()
		if err == io.EOF {
			if err := i.closeReader(); err != nil {
				return i.fail(i.inputError(ctx, err))
			}
			i.next++
			continue
		}
		if err != nil {
			return i.fail(i.inputError(ctx, err))
		}
		return KeyValue{Key: record.Key, Value: record.Value}, true, nil
	}
}

func (i *shuffleIterator) inputError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, shuffle.ErrShuffleInput) {
		return err
	}
	return &shuffle.InputError{Attempt: i.inputs.Outputs[i.next].Attempt, PartitionID: i.partition, Err: err}
}

func (i *shuffleIterator) fail(err error) (Record, bool, error) {
	i.err = errors.Join(err, i.Close())
	return nil, false, i.err
}

func (i *shuffleIterator) closeReader() error {
	if i.reader == nil {
		return nil
	}
	reader := i.reader
	i.reader = nil
	return reader.Close()
}

func (i *shuffleIterator) Close() error {
	i.closed = true
	return i.closeReader()
}
