package executor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestLocalRunnerExecutesCompleteNarrowPipelineForCount(t *testing.T) {
	registry := NewFunctionRegistry()
	var maps atomic.Int64
	var filters atomic.Int64
	mustRegisterRunnerMap(t, registry, "double", func(record Record) (Record, error) {
		maps.Add(1)
		return record.(int) * 2, nil
	})
	mustRegisterRunnerFilter(t, registry, "over-four", func(record Record) (bool, error) {
		filters.Add(1)
		return record.(int) > 4, nil
	})
	sources := memorySourceReader{partitions: map[plan.PartitionID][]Record{
		0: {1, 2}, 1: {3}, 2: {}, 3: {4, 5},
	}}
	tasks := narrowTasks(t, scheduler.ActionCount, 4)

	result, err := NewLocalRunner(registry, sources, 2).Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Count != 3 {
		t.Fatalf("Run() count = %d, want 3", result.Count)
	}
	if maps.Load() != 5 || filters.Load() != 5 {
		t.Fatalf("pipeline calls = map:%d filter:%d, want 5 each", maps.Load(), filters.Load())
	}
}

func TestLocalRunnerCountsFourTextFilePartitions(t *testing.T) {
	path := writeTextFixture(t, "keep\nskip\nkeep\nkeep\nskip\nkeep\n")
	registry := NewFunctionRegistry()
	mustRegisterRunnerMap(t, registry, "double", func(record Record) (Record, error) {
		return record, nil
	})
	mustRegisterRunnerFilter(t, registry, "over-four", func(record Record) (bool, error) {
		return record == "keep", nil
	})
	tasks := narrowTasks(t, scheduler.ActionCount, 4)
	for i := range tasks {
		tasks[i].Operations[0].RDD.Operator.SourcePath = path
	}

	result, err := NewLocalRunner(registry, TextSourceReader{}, 4).Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Count != 4 {
		t.Fatalf("Run() count = %d, want 4", result.Count)
	}
}

func TestLocalRunnerCollectMergesInPartitionOrder(t *testing.T) {
	registry := NewFunctionRegistry()
	mustRegisterRunnerMap(t, registry, "double", func(record Record) (Record, error) { return record, nil })
	mustRegisterRunnerFilter(t, registry, "over-four", func(Record) (bool, error) { return true, nil })
	sources := memorySourceReader{partitions: map[plan.PartitionID][]Record{
		0: {"p0-a", "p0-b"}, 1: {"p1"}, 2: {}, 3: {"p3"},
	}}

	result, err := NewLocalRunner(registry, sources, 4).Run(
		context.Background(), narrowTasks(t, scheduler.ActionCollect, 4),
	)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []Record{"p0-a", "p0-b", "p1", "p3"}
	if !reflect.DeepEqual(result.Records, want) {
		t.Fatalf("Run() records = %#v, want %#v", result.Records, want)
	}
}

func TestLocalRunnerCancellationStopsWork(t *testing.T) {
	registry := NewFunctionRegistry()
	mustRegisterRunnerMap(t, registry, "double", func(record Record) (Record, error) { return record, nil })
	mustRegisterRunnerFilter(t, registry, "over-four", func(Record) (bool, error) { return true, nil })
	ctx, cancel := context.WithCancel(context.Background())
	sources := blockingSourceReader{started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := NewLocalRunner(registry, sources, 2).Run(ctx, narrowTasks(t, scheduler.ActionCount, 4))
		done <- err
	}()
	<-sources.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func TestLocalRunnerErrorsIdentifyStageAndPartition(t *testing.T) {
	registry := NewFunctionRegistry()
	mustRegisterRunnerMap(t, registry, "double", func(record Record) (Record, error) { return record, nil })
	mustRegisterRunnerFilter(t, registry, "over-four", func(Record) (bool, error) { return true, nil })
	tasks := narrowTasks(t, scheduler.ActionCount, 1)

	_, err := NewLocalRunner(registry, failingSourceReader{}, 1).Run(context.Background(), tasks)
	if err == nil || !strings.Contains(err.Error(), "stage 0 partition 0") {
		t.Fatalf("Run() error = %v, want stage and partition", err)
	}
}

func narrowTasks(t *testing.T, action scheduler.ActionKind, partitions int) []scheduler.Task {
	t.Helper()
	stagePlan := scheduler.StagePlan{Stages: []scheduler.Stage{{
		ID:            0,
		Kind:          scheduler.StageResult,
		NumPartitions: partitions,
		Operations: []scheduler.StageOperation{
			runnerOperation(0, plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "unused"}),
			runnerOperation(1, plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "double"}),
			runnerOperation(2, plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "over-four"}),
		},
		FinalAction: &scheduler.ActionSpec{Kind: action, TargetRDD: 2},
	}}}
	tasks, err := scheduler.GenerateTasks(stagePlan)
	if err != nil {
		t.Fatalf("GenerateTasks() error = %v", err)
	}
	return tasks
}

func runnerOperation(id plan.RDDID, operator plan.OperatorSpec) scheduler.StageOperation {
	return scheduler.StageOperation{
		Kind: scheduler.StageOperationRDD,
		RDD:  &scheduler.RDDOperationSpec{RDDID: id, Operator: operator},
	}
}

type memorySourceReader struct{ partitions map[plan.PartitionID][]Record }

func (r memorySourceReader) Open(_ context.Context, _ string, partition plan.PartitionID, _ int) (Iterator, error) {
	return &sliceIterator{records: r.partitions[partition]}, nil
}

type failingSourceReader struct{}

func (failingSourceReader) Open(context.Context, string, plan.PartitionID, int) (Iterator, error) {
	return nil, errors.New("source failed")
}

type blockingSourceReader struct{ started chan struct{} }

func (r blockingSourceReader) Open(ctx context.Context, _ string, _ plan.PartitionID, _ int) (Iterator, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	return blockingIterator{}, nil
}

type blockingIterator struct{}

func (blockingIterator) Next(ctx context.Context) (Record, bool, error) {
	<-ctx.Done()
	return nil, false, ctx.Err()
}

func mustRegisterRunnerMap(t *testing.T, registry *FunctionRegistry, id string, fn MapFunc) {
	t.Helper()
	if err := registry.RegisterMap(id, fn); err != nil {
		t.Fatalf("RegisterMap() error = %v", err)
	}
}

func mustRegisterRunnerFilter(t *testing.T, registry *FunctionRegistry, id string, fn FilterFunc) {
	t.Helper()
	if err := registry.RegisterFilter(id, fn); err != nil {
		t.Fatalf("RegisterFilter() error = %v", err)
	}
}
