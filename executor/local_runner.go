package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

// LocalRunner executes partition tasks in-process with bounded concurrency.
type LocalRunner struct {
	registry       *FunctionRegistry
	sources        SourceReader
	maxConcurrency int
	permits        chan struct{}
	shuffleStore   shuffle.Store
}

var (
	_ scheduler.TaskRunner     = (*LocalRunner)(nil)
	_ scheduler.FunctionLookup = (*FunctionRegistry)(nil)
)

// RunnerOption configures an optional runner dependency.
type RunnerOption func(*LocalRunner)

// WithShuffleStore enables shuffle execution. The caller owns the store's lifetime.
func WithShuffleStore(store shuffle.Store) RunnerOption {
	return func(r *LocalRunner) { r.shuffleStore = store }
}

// NewLocalRunner creates an in-process task runner.
func NewLocalRunner(registry *FunctionRegistry, sources SourceReader, maxConcurrency int, options ...RunnerOption) *LocalRunner {
	if sources == nil {
		sources = TextSourceReader{}
	}
	var permits chan struct{}
	if maxConcurrency > 0 {
		permits = make(chan struct{}, maxConcurrency)
	}
	runner := &LocalRunner{
		registry:       registry,
		sources:        sources,
		maxConcurrency: maxConcurrency,
		permits:        permits,
	}
	for _, option := range options {
		option(runner)
	}
	return runner
}

type partitionResult struct {
	records []Record
	count   int64
}

func (r *LocalRunner) runTask(ctx context.Context, execution scheduler.TaskExecution) (result partitionResult, err error) {
	task := execution.Task
	if task.StageKind != scheduler.StageResult || task.FinalAction == nil {
		return partitionResult{}, fmt.Errorf("result task requires a result stage and final action")
	}
	if task.ShuffleWrite != nil {
		return partitionResult{}, fmt.Errorf("result task must not have a shuffle write")
	}

	iterator, err := buildTaskIterator(ctx, execution, r.registry, r.sources, r.shuffleStore)
	if err != nil {
		return partitionResult{}, err
	}
	defer func() { err = errors.Join(err, closeIterator(iterator)) }()
	for {
		record, ok, err := iterator.Next(ctx)
		if err != nil {
			return partitionResult{}, err
		}
		if !ok {
			return result, nil
		}
		switch task.FinalAction.Kind {
		case scheduler.ActionCollect:
			result.records = append(result.records, record)
		case scheduler.ActionCount:
			result.count++
		default:
			return partitionResult{}, fmt.Errorf("unsupported action %q", task.FinalAction.Kind)
		}
	}
}

// RunTask executes one attempt while respecting the runner's concurrency limit.
func (r *LocalRunner) RunTask(ctx context.Context, execution scheduler.TaskExecution) (scheduler.TaskOutput, error) {
	if ctx == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("task context is nil")
	}
	if err := execution.Validate(); err != nil {
		return scheduler.TaskOutput{}, err
	}
	if r == nil || r.registry == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner function registry is nil")
	}
	if r.sources == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner source reader is nil")
	}
	if r.maxConcurrency <= 0 || r.permits == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner concurrency must be positive")
	}
	if err := validateShuffleExecution(execution, r.shuffleStore); err != nil {
		return scheduler.TaskOutput{}, err
	}
	select {
	case r.permits <- struct{}{}:
		defer func() { <-r.permits }()
	case <-ctx.Done():
		return scheduler.TaskOutput{}, context.Cause(ctx)
	}
	if err := context.Cause(ctx); err != nil {
		return scheduler.TaskOutput{}, err
	}
	if execution.Task.StageKind == scheduler.StageShuffleMap {
		return r.runShuffleMap(ctx, execution)
	}

	result, err := r.runTask(ctx, execution)
	if err != nil {
		return scheduler.TaskOutput{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return scheduler.TaskOutput{}, err
	}
	var records []any
	if execution.Task.FinalAction.Kind == scheduler.ActionCollect {
		records = make([]any, len(result.records))
	}
	for i, record := range result.records {
		records[i] = record
	}
	return scheduler.TaskOutput{Records: records, Count: result.count}, nil
}
