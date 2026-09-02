package scheduler

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestDAGSchedulerCompletesCountAfterEveryPartitionSucceeds(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(4))
	runner := newControlledTaskRunner()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, runner)
	defer dag.Close()

	resultCh := make(chan JobResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		resultCh <- result
		errCh <- err
	}()
	runner.waitForTasks(t, 4)

	for partition := plan.PartitionID(0); partition < 3; partition++ {
		runner.succeed(partition, TaskOutput{Count: 1})
	}
	select {
	case result := <-resultCh:
		t.Fatalf("job completed early with %#v", result)
	case <-time.After(20 * time.Millisecond):
	}

	runner.succeed(3, TaskOutput{Count: 1})
	if err := <-errCh; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result := <-resultCh; result.Count != 4 {
		t.Fatalf("Run() count = %d, want 4", result.Count)
	}
}

func TestDAGSchedulerCollectsInPartitionOrder(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(3))
	runner := newControlledTaskRunner()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, runner)
	defer dag.Close()

	resultCh := make(chan JobResult, 1)
	go func() {
		result, _ := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCollect, TargetRDD: target})
		resultCh <- result
	}()
	runner.waitForTasks(t, 3)
	runner.succeed(2, TaskOutput{Records: []any{"p2"}})
	runner.succeed(0, TaskOutput{Records: []any{"p0"}})
	runner.succeed(1, TaskOutput{Records: []any{"p1"}})

	result := <-resultCh
	if want := []any{"p0", "p1", "p2"}; !reflect.DeepEqual(result.Records, want) {
		t.Fatalf("Run() records = %#v, want %#v", result.Records, want)
	}
}

func TestDAGSchedulerRejectsShuffleExecutionBeforeDispatch(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(4))
	paired := addPlannerNode(t, graph, plannerNarrowNode("MapToPair", plan.OpMapToPair, "pair", source, 4))
	target := addPlannerNode(t, graph, plannerShuffleNode("ReduceByKey", "sum", paired, plan.HashPartitioner(2), 3))
	runner := newControlledTaskRunner()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, runner)
	defer dag.Close()

	_, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCollect, TargetRDD: target})
	if err == nil || !strings.Contains(err.Error(), "shuffle execution is not implemented") {
		t.Fatalf("Run() error = %v, want unsupported shuffle", err)
	}
	if runner.taskCount() != 0 {
		t.Fatalf("dispatched tasks = %d, want 0", runner.taskCount())
	}
}

func TestDAGSchedulerFailureCancelsSiblingTasks(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(3))
	runner := newControlledTaskRunner()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, runner)
	defer dag.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		errCh <- err
	}()
	runner.waitForTasks(t, 3)
	runner.fail(1, errors.New("broken partition"))

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "stage 0 task 1 partition 1") {
		t.Fatalf("Run() error = %v, want task identity", err)
	}
	runner.waitForCancellations(t, 2)
}

type controlledTaskRunner struct {
	mu       sync.Mutex
	controls map[plan.PartitionID]chan controlledTaskResult
	started  chan plan.PartitionID
	canceled chan plan.PartitionID
}

type controlledTaskResult struct {
	output TaskOutput
	err    error
}

func newControlledTaskRunner() *controlledTaskRunner {
	return &controlledTaskRunner{
		controls: make(map[plan.PartitionID]chan controlledTaskResult),
		started:  make(chan plan.PartitionID, 16),
		canceled: make(chan plan.PartitionID, 16),
	}
}

func (r *controlledTaskRunner) RunTask(ctx context.Context, task Task) (TaskOutput, error) {
	control := make(chan controlledTaskResult, 1)
	r.mu.Lock()
	r.controls[task.PartitionID] = control
	r.mu.Unlock()
	r.started <- task.PartitionID
	select {
	case result := <-control:
		return result.output, result.err
	case <-ctx.Done():
		r.canceled <- task.PartitionID
		return TaskOutput{}, ctx.Err()
	}
}

func (r *controlledTaskRunner) waitForTasks(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-r.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for dispatched tasks")
		}
	}
}

func (r *controlledTaskRunner) succeed(partition plan.PartitionID, output TaskOutput) {
	r.complete(partition, controlledTaskResult{output: output})
}

func (r *controlledTaskRunner) fail(partition plan.PartitionID, err error) {
	r.complete(partition, controlledTaskResult{err: err})
}

func (r *controlledTaskRunner) complete(partition plan.PartitionID, result controlledTaskResult) {
	r.mu.Lock()
	control := r.controls[partition]
	r.mu.Unlock()
	control <- result
}

func (r *controlledTaskRunner) taskCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.controls)
}

func (r *controlledTaskRunner) waitForCancellations(t *testing.T, count int) {
	t.Helper()
	for range count {
		select {
		case <-r.canceled:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for sibling cancellation")
		}
	}
}
