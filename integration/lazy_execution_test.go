package integration_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestPublicAPINarrowPipelineIsLazyUntilCount(t *testing.T) {
	registry := executor.NewFunctionRegistry()
	mustRegisterMap(t, registry, "normalize", func(record executor.Record) (executor.Record, error) {
		return strings.TrimSpace(record.(string)), nil
	})
	mustRegisterFilter(t, registry, "non-empty", func(record executor.Record) (bool, error) {
		return record.(string) != "", nil
	})

	source := &spySource{partitions: map[plan.PartitionID][]executor.Record{
		0: {" alpha ", "  "},
		1: {"beta"},
		2: {},
		3: {" gamma", "delta "},
	}}
	localRunner := executor.NewLocalRunner(registry, source, 2)
	dagScheduler := scheduler.NewDAGScheduler(registry, localRunner)
	t.Cleanup(dagScheduler.Close)
	ctx := api.NewContext(registry, executor.NewSchedulerActionRunner(dagScheduler))

	rdd, err := ctx.TextFile("spy://lazy-input", 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	rdd, err = rdd.Map("normalize")
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	rdd, err = rdd.Filter("non-empty")
	if err != nil {
		t.Fatalf("Filter() error = %v", err)
	}
	if got := source.Reads(); got != 0 {
		t.Fatalf("source reads after transformations = %d, want 0", got)
	}

	explanation, err := rdd.ExplainLineage()
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	if !strings.Contains(explanation, "Map") || !strings.Contains(explanation, "Filter") {
		t.Fatalf("ExplainLineage() = %q, want Map and Filter", explanation)
	}
	if got := source.Reads(); got != 0 {
		t.Fatalf("source reads after ExplainLineage = %d, want 0", got)
	}

	count, err := rdd.Count(context.Background())
	if err != nil {
		t.Fatalf("Count() error = %v", err)
	}
	if count != 4 {
		t.Fatalf("Count() = %d, want 4", count)
	}
	if got := source.Reads(); got != 4 {
		t.Fatalf("source reads after Count = %d, want one per partition (4)", got)
	}
}

type spySource struct {
	mu         sync.Mutex
	reads      int
	partitions map[plan.PartitionID][]executor.Record
}

func (s *spySource) Open(
	_ context.Context,
	_ string,
	partition plan.PartitionID,
	_ int,
) (executor.Iterator, error) {
	s.mu.Lock()
	s.reads++
	records := append([]executor.Record(nil), s.partitions[partition]...)
	s.mu.Unlock()
	return &recordIterator{records: records}, nil
}

func (s *spySource) Reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

type recordIterator struct {
	records []executor.Record
	next    int
}

func (i *recordIterator) Next(ctx context.Context) (executor.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if i.next == len(i.records) {
		return nil, false, nil
	}
	record := i.records[i.next]
	i.next++
	return record, true, nil
}

func mustRegisterMap(t *testing.T, registry *executor.FunctionRegistry, id string, fn executor.MapFunc) {
	t.Helper()
	if err := registry.RegisterMap(id, fn); err != nil {
		t.Fatalf("RegisterMap(%q) error = %v", id, err)
	}
}

func mustRegisterFilter(t *testing.T, registry *executor.FunctionRegistry, id string, fn executor.FilterFunc) {
	t.Helper()
	if err := registry.RegisterFilter(id, fn); err != nil {
		t.Fatalf("RegisterFilter(%q) error = %v", id, err)
	}
}
