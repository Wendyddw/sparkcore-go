package api

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
)

type recordingActionRunner struct {
	collectRecords []executor.Record
	collectErr     error
	count          int64
	countErr       error

	collectCalls int
	countCalls   int
	ctx          context.Context
	graph        *plan.RDDGraph
	target       plan.RDDID
}

func (r *recordingActionRunner) Collect(
	ctx context.Context,
	graph *plan.RDDGraph,
	target plan.RDDID,
) ([]executor.Record, error) {
	r.collectCalls++
	r.ctx = ctx
	r.graph = graph
	r.target = target
	return r.collectRecords, r.collectErr
}

func (r *recordingActionRunner) Count(
	ctx context.Context,
	graph *plan.RDDGraph,
	target plan.RDDID,
) (int64, error) {
	r.countCalls++
	r.ctx = ctx
	r.graph = graph
	r.target = target
	return r.count, r.countErr
}

func TestCollectDelegatesToActionRunner(t *testing.T) {
	runner := &recordingActionRunner{
		collectRecords: []executor.Record{"first", "second"},
	}
	driver := NewContext(nil, runner)
	rdd, err := driver.TextFile("missing.txt", 2)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	callContext := context.WithValue(context.Background(), actionContextKey{}, "collect")

	records, err := rdd.Collect(callContext)
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if !reflect.DeepEqual(records, runner.collectRecords) {
		t.Errorf("Collect() = %#v, want %#v", records, runner.collectRecords)
	}
	assertRunnerCall(t, runner.collectCalls, runner.ctx, runner.graph, runner.target, callContext, driver.graph, rdd.id)
	if runner.countCalls != 0 {
		t.Errorf("Count() calls = %d, want 0", runner.countCalls)
	}
}

func TestCountDelegatesToActionRunner(t *testing.T) {
	runner := &recordingActionRunner{count: 42}
	driver := NewContext(nil, runner)
	rdd, err := driver.TextFile("missing.txt", 2)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	callContext := context.WithValue(context.Background(), actionContextKey{}, "count")

	count, err := rdd.Count(callContext)
	if err != nil {
		t.Fatalf("Count() error = %v", err)
	}
	if count != 42 {
		t.Errorf("Count() = %d, want 42", count)
	}
	assertRunnerCall(t, runner.countCalls, runner.ctx, runner.graph, runner.target, callContext, driver.graph, rdd.id)
	if runner.collectCalls != 0 {
		t.Errorf("Collect() calls = %d, want 0", runner.collectCalls)
	}
}

func TestActionsWrapRunnerErrors(t *testing.T) {
	sentinel := errors.New("planning failed")
	runner := &recordingActionRunner{collectErr: sentinel, countErr: sentinel}
	driver := NewContext(nil, runner)
	rdd, err := driver.TextFile("missing.txt", 1)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}

	if _, err := rdd.Collect(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("Collect() error = %v, want wrapped sentinel", err)
	}
	if _, err := rdd.Count(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("Count() error = %v, want wrapped sentinel", err)
	}
}

func TestActionsRejectMissingDependencies(t *testing.T) {
	if _, err := (RDD{}).Collect(context.Background()); err == nil || !strings.Contains(err.Error(), "missing context") {
		t.Errorf("Collect() error = %v, want missing context error", err)
	}
	if _, err := (RDD{}).Count(context.Background()); err == nil || !strings.Contains(err.Error(), "missing context") {
		t.Errorf("Count() error = %v, want missing context error", err)
	}

	rdd, err := NewContext(nil, nil).TextFile("missing.txt", 1)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	if _, err := rdd.Collect(context.Background()); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Errorf("Collect() error = %v, want runner configuration error", err)
	}
	if _, err := rdd.Count(context.Background()); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Errorf("Count() error = %v, want runner configuration error", err)
	}
}

type actionContextKey struct{}

func assertRunnerCall(
	t *testing.T,
	calls int,
	gotContext context.Context,
	gotGraph *plan.RDDGraph,
	gotTarget plan.RDDID,
	wantContext context.Context,
	wantGraph *plan.RDDGraph,
	wantTarget plan.RDDID,
) {
	t.Helper()
	if calls != 1 {
		t.Errorf("runner calls = %d, want 1", calls)
	}
	if gotContext != wantContext {
		t.Error("runner received a different context")
	}
	if gotGraph != wantGraph {
		t.Error("runner received a different lineage graph")
	}
	if gotTarget != wantTarget {
		t.Errorf("runner target = %d, want %d", gotTarget, wantTarget)
	}
}
