package scheduler

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

type taskSetSchedulerFunc func(context.Context, TaskSet, TaskSetObserver) error

func (f taskSetSchedulerFunc) ScheduleTaskSet(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
	return f(ctx, set, observer)
}

func successFor(set TaskSet, partition int, output TaskOutput) TaskAttemptSuccess {
	task := set.Tasks[partition]
	return TaskAttemptSuccess{JobID: set.JobID, StageID: set.StageID,
		Attempt:     TaskAttemptIdentity{ID: plan.TaskAttemptID(partition), TaskID: task.ID, StageAttemptID: set.StageAttemptID},
		PartitionID: task.PartitionID, WorkerID: "test-worker", Output: output}
}

func TestDAGSchedulerSubmitsTaskSetAndMergesEveryPartition(t *testing.T) {
	for _, action := range []ActionKind{ActionCount, ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
			graph := plan.NewRDDGraph()
			target := addPlannerNode(t, graph, plannerSourceNode(4))
			var submitted TaskSet
			physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
				submitted = set
				if len(set.Tasks) != 4 {
					return errors.New("expected four tasks")
				}
				for _, partition := range []int{2, 0, 1, 3} {
					report := successFor(set, partition, TaskOutput{Count: int64(partition + 1), Records: []any{partition}})
					observer.TaskSucceeded(report)
					// Duplicate reports must not count as another logical partition.
					observer.TaskSucceeded(report)
				}
				return nil
			})
			dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
			defer dag.Close()
			result, err := dag.Run(context.Background(), graph, ActionSpec{Kind: action, TargetRDD: target})
			if err != nil {
				t.Fatal(err)
			}
			if action == ActionCount && result.Count != 10 {
				t.Fatalf("count = %d, want 10", result.Count)
			}
			if action == ActionCollect && !reflect.DeepEqual(result.Records, []any{0, 1, 2, 3}) {
				t.Fatalf("records = %v", result.Records)
			}
			for i, task := range submitted.Tasks {
				if task.PartitionID != plan.PartitionID(i) || task.StageID != submitted.StageID {
					t.Fatalf("task %d = %#v", i, task)
				}
			}
		})
	}
}

func TestDAGSchedulerIgnoresMismatchedReports(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(2))
	physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
		valid := successFor(set, 0, TaskOutput{Count: 1})
		for _, mutate := range []func(*TaskAttemptSuccess){
			func(r *TaskAttemptSuccess) { r.JobID++ },
			func(r *TaskAttemptSuccess) { r.StageID++ },
			func(r *TaskAttemptSuccess) { r.Attempt.StageAttemptID++ },
			func(r *TaskAttemptSuccess) { r.Attempt.TaskID++ },
			func(r *TaskAttemptSuccess) { r.PartitionID = 99 },
		} {
			bad := valid
			mutate(&bad)
			bad.Output.Count = 100
			observer.TaskSucceeded(bad)
			observer.TaskFailed(TaskAttemptFailure{JobID: bad.JobID, StageID: bad.StageID, Attempt: bad.Attempt, PartitionID: bad.PartitionID, Error: "obsolete failure"})
		}
		observer.TaskSucceeded(valid)
		observer.TaskFailed(TaskAttemptFailure{JobID: valid.JobID, StageID: valid.StageID, Attempt: valid.Attempt, PartitionID: valid.PartitionID, Error: "duplicate terminal report"})
		observer.TaskSucceeded(successFor(set, 1, TaskOutput{Count: 2}))
		return nil
	})
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
	defer dag.Close()
	result, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
	if err != nil || result.Count != 3 {
		t.Fatalf("Run() = %#v, %v; want count 3", result, err)
	}
}

func TestDAGSchedulerSchedulingErrorsUnblockJob(t *testing.T) {
	sentinel := errors.New("scheduling unavailable")
	for _, schedulingErr := range []error{sentinel, nil} {
		graph := plan.NewRDDGraph()
		target := addPlannerNode(t, graph, plannerSourceNode(2))
		physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error { return schedulingErr })
		dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
		_, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		dag.Close()
		if err == nil {
			t.Fatal("expected error for scheduling ending without partition outcomes")
		}
		if schedulingErr != nil && !errors.Is(err, sentinel) {
			t.Fatalf("error = %v, want scheduling error", err)
		}
	}
}

func TestDAGSchedulerRejectsShuffleExecutionBeforeDispatch(t *testing.T) {
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(4))
	paired := addPlannerNode(t, graph, plannerNarrowNode("MapToPair", plan.OpMapToPair, "pair", source, 4))
	target := addPlannerNode(t, graph, plannerShuffleNode("ReduceByKey", "sum", paired, plan.HashPartitioner(2), 3))
	called := false
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, taskSetSchedulerFunc(func(context.Context, TaskSet, TaskSetObserver) error { called = true; return nil }))
	_, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCollect, TargetRDD: target})
	dag.Close()
	if err == nil || !strings.Contains(err.Error(), "shuffle execution is not implemented") || called {
		t.Fatalf("error = %v, dispatched = %v", err, called)
	}
}

func TestDAGSchedulerCancellationAndFailureStopScheduling(t *testing.T) {
	for _, reason := range []string{"failure", "caller", "close"} {
		t.Run(reason, func(t *testing.T) {
			graph := plan.NewRDDGraph()
			target := addPlannerNode(t, graph, plannerSourceNode(3))
			started, exited := make(chan struct{}), make(chan struct{})
			physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
				defer close(exited)
				close(started)
				if reason == "failure" {
					r := successFor(set, 1, TaskOutput{})
					observer.TaskFailed(TaskAttemptFailure{JobID: r.JobID, StageID: r.StageID, Attempt: r.Attempt, PartitionID: r.PartitionID, Error: "broken partition"})
				}
				<-ctx.Done()
				return ctx.Err()
			})
			dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
			defer dag.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := dag.Run(ctx, graph, ActionSpec{Kind: ActionCount, TargetRDD: target}); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("scheduling did not start")
			}
			if reason == "caller" {
				cancel()
			}
			if reason == "close" {
				dag.Close()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected error")
				}
				if reason == "caller" && !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
				if reason == "close" && !errors.Is(err, ErrSchedulerClosed) {
					t.Fatalf("error = %v", err)
				}
				if reason == "failure" && !strings.Contains(err.Error(), "stage 0 task 1 partition 1") {
					t.Fatalf("error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("job did not unblock")
			}
			dag.Close()
			select {
			case <-exited:
			default:
				t.Fatal("Close returned before scheduling exited")
			}
		})
	}
}

func TestDAGSchedulerCloseIsIdempotentAndRejectsNewJobs(t *testing.T) {
	dag := NewDAGScheduler(nil, nil)
	dag.Close()
	dag.Close()
	_, err := dag.Run(context.Background(), plan.NewRDDGraph(), ActionSpec{Kind: ActionCount})
	if !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("error = %v", err)
	}
}

func TestDAGSchedulerRejectsMissingTaskScheduler(t *testing.T) {
	dag := NewDAGScheduler(nil, nil)
	defer dag.Close()
	_, err := dag.Run(context.Background(), plan.NewRDDGraph(), ActionSpec{Kind: ActionCount})
	if err == nil || !strings.Contains(err.Error(), "task scheduler is nil") {
		t.Fatalf("error = %v", err)
	}
}

func TestDAGSchedulerKeepsConcurrentTaskSetsIsolated(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(1))
	type submission struct {
		set      TaskSet
		observer TaskSetObserver
	}
	submissions := make(chan submission, 2)
	physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
		submissions <- submission{set, observer}
		<-ctx.Done()
		return ctx.Err()
	})
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
	defer dag.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan JobResult, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			result, err := dag.Run(ctx, graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
			results <- result
			errs <- err
		}()
	}
	received := make([]submission, 0, 2)
	for range 2 {
		select {
		case item := <-submissions:
			received = append(received, item)
		case <-ctx.Done():
			t.Fatal("task sets did not start concurrently")
		}
	}
	first, second := received[0], received[1]
	if first.set.JobID == second.set.JobID || first.set.StageAttemptID == second.set.StageAttemptID {
		t.Fatal("task sets share job or stage-attempt identity")
	}
	// A report through the wrong submission's observer cannot complete the other job.
	first.observer.TaskSucceeded(successFor(second.set, 0, TaskOutput{Count: 100}))
	first.observer.TaskSucceeded(successFor(first.set, 0, TaskOutput{Count: 1}))
	second.observer.TaskSucceeded(successFor(second.set, 0, TaskOutput{Count: 2}))
	var total int64
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		total += (<-results).Count
	}
	if total != 3 {
		t.Fatalf("combined count = %d, want 3", total)
	}
}
