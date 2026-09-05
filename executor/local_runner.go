package executor

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// LocalRunner executes result tasks in-process with bounded concurrency.
type LocalRunner struct {
	registry       *FunctionRegistry
	sources        SourceReader
	maxConcurrency int
	permits        chan struct{}
}

// NewLocalRunner creates an in-process task runner.
func NewLocalRunner(registry *FunctionRegistry, sources SourceReader, maxConcurrency int) *LocalRunner {
	if sources == nil {
		sources = TextSourceReader{}
	}
	var permits chan struct{}
	if maxConcurrency > 0 {
		permits = make(chan struct{}, maxConcurrency)
	}
	return &LocalRunner{
		registry:       registry,
		sources:        sources,
		maxConcurrency: maxConcurrency,
		permits:        permits,
	}
}

type partitionResult struct {
	records []Record
	count   int64
}

func (r *LocalRunner) runTask(ctx context.Context, task scheduler.Task) (partitionResult, error) {
	if task.StageKind != scheduler.StageResult || task.FinalAction == nil {
		return partitionResult{}, fmt.Errorf("only result-stage tasks are executable locally")
	}
	if task.ShuffleWrite != nil {
		return partitionResult{}, fmt.Errorf("shuffle write is not implemented in Week 1")
	}

	iterator, err := buildTaskIterator(ctx, task, r.registry, r.sources)
	if err != nil {
		return partitionResult{}, err
	}
	var result partitionResult
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

// RunTask executes one partition while respecting the runner's concurrency limit.
func (r *LocalRunner) RunTask(ctx context.Context, task scheduler.Task) (scheduler.TaskOutput, error) {
	if r == nil || r.registry == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner function registry is nil")
	}
	if r.sources == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner source reader is nil")
	}
	if r.maxConcurrency <= 0 || r.permits == nil {
		return scheduler.TaskOutput{}, fmt.Errorf("local runner concurrency must be positive")
	}
	select {
	case r.permits <- struct{}{}:
		defer func() { <-r.permits }()
	case <-ctx.Done():
		return scheduler.TaskOutput{}, ctx.Err()
	}

	result, err := r.runTask(ctx, task)
	if err != nil {
		return scheduler.TaskOutput{}, err
	}
	records := make([]any, len(result.records))
	for i, record := range result.records {
		records[i] = record
	}
	return scheduler.TaskOutput{Records: records, Count: result.count}, nil
}
