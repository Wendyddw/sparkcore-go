package coordinator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

type observedTaskScheduler struct {
	*scheduler.FIFOTaskScheduler
	sets chan scheduler.TaskSet
}

func (s *observedTaskScheduler) ScheduleTaskSet(ctx context.Context, set scheduler.TaskSet, observer scheduler.TaskSetObserver) error {
	s.sets <- set
	return s.FIFOTaskScheduler.ScheduleTaskSet(ctx, set, observer)
}

func newJobService(t *testing.T) (*coordinator.JobService, *observedTaskScheduler, http.Handler) {
	t.Helper()
	tasks, server := newService(t, coordinator.Config{})
	observed := &observedTaskScheduler{FIFOTaskScheduler: tasks, sets: make(chan scheduler.TaskSet, 16)}
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		t.Fatal(err)
	}
	jobs, err := coordinator.NewJobService(registry, observed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(jobs.Close)
	return jobs, observed, server.Handler
}

func narrowJob(t *testing.T, action scheduler.ActionKind, partitions int) jobspec.Spec {
	t.Helper()
	return jobspec.Spec{
		Source: jobspec.SourceSpec{Path: filepath.Join(t.TempDir(), "unopened.txt"), NumPartitions: partitions},
		Transformations: []jobspec.TransformationSpec{
			{Kind: plan.OpMap, FunctionID: "normalize"},
			{Kind: plan.OpFilter, FunctionID: "non_empty"},
		},
		Action: action,
	}
}

type jobCompletion struct {
	result protocol.JobResultResponse
	err    error
}

func startJob(t *testing.T, jobs *coordinator.JobService, ctx context.Context, spec jobspec.Spec) <-chan jobCompletion {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan jobCompletion, 1)
	go func() {
		result, err := jobs.Submit(ctx, spec)
		done <- jobCompletion{result: result, err: err}
	}()
	return done
}

func waitJob(t *testing.T, done <-chan jobCompletion) jobCompletion {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("job did not finish")
		return jobCompletion{}
	}
}

func assertJobPending(t *testing.T, done <-chan jobCompletion) {
	t.Helper()
	select {
	case result := <-done:
		t.Fatalf("job finished before all partitions succeeded: %+v", result)
	default:
	}
}

func TestJobServiceMergesRemotePartitions(t *testing.T) {
	for _, action := range []scheduler.ActionKind{scheduler.ActionCount, scheduler.ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
			jobs, tasks, h := newJobService(t)
			register(t, h, "a", 2)
			register(t, h, "b", 2)
			spec := narrowJob(t, action, 4)
			done := startJob(t, jobs, context.Background(), spec)
			assigned := awaitAssignments(t, h, "a", 2)
			assigned = append(assigned, awaitAssignments(t, h, "b", 2)...)
			if len(assigned) != 4 {
				t.Fatalf("assignments = %d, want 4", len(assigned))
			}
			set := <-tasks.sets
			if len(set.Tasks) != 4 {
				t.Fatalf("task set has %d tasks", len(set.Tasks))
			}
			for i, a := range assigned {
				if a.JobID != set.JobID || a.StageID != set.StageID || a.Attempt.StageAttemptID != set.StageAttemptID ||
					a.Task.PartitionID != plan.PartitionID(i) || a.Task.NumPartitions != 4 ||
					len(a.Task.Operations) != 3 || a.Task.Operations[0].RDD.Operator.SourcePath != spec.Source.Path ||
					a.Task.FinalAction.Kind != action {
					t.Fatalf("unexpected assignment: %+v", a)
				}
			}
			outputs := []protocol.TaskOutput{{Count: 9007199254740993}, {Count: 2}, {Count: 0}, {Count: 7}}
			if action == scheduler.ActionCollect {
				outputs = []protocol.TaskOutput{
					{Records: []json.RawMessage{json.RawMessage(`9007199254740993`), json.RawMessage(`"first"`)}},
					{Records: []json.RawMessage{}},
					{Records: []json.RawMessage{json.RawMessage(`{"key":"two","value":9007199254740995}`)}},
					{Records: []json.RawMessage{json.RawMessage(`null`)}},
				}
			}
			for _, partition := range []int{2, 0, 3} {
				r := successReport(assigned[partition], outputs[partition])
				acknowledge(t, h, r)
				acknowledge(t, h, r)
			}
			assertJobPending(t, done)
			stale := successReport(assigned[1], outputs[1])
			stale.Attempt.StageAttemptID++
			checkError(t, postReport(t, h, stale), http.StatusConflict, protocol.CodeConflict)
			assertJobPending(t, done)
			acknowledge(t, h, successReport(assigned[1], outputs[1]))
			got := waitJob(t, done)
			if got.err != nil || got.result.Action != action {
				t.Fatalf("result = %+v", got)
			}
			if action == scheduler.ActionCount {
				if got.result.Count != 9007199254741002 || got.result.Records != nil {
					t.Fatalf("count result = %+v", got.result)
				}
			} else {
				want := []json.RawMessage{outputs[0].Records[0], outputs[0].Records[1], outputs[2].Records[0], outputs[3].Records[0]}
				if got.result.Count != 0 || !reflect.DeepEqual(got.result.Records, want) {
					t.Fatalf("collect = %s, want %s", got.result.Records, want)
				}
			}
			if err := got.result.Validate(); err != nil {
				t.Fatal(err)
			}
			select {
			case extra := <-tasks.sets:
				t.Fatalf("narrow job scheduled another task set: %+v", extra)
			default:
			}
		})
	}
}

func TestJobServiceConcurrentJobsOwnTheirLineage(t *testing.T) {
	jobs, _, h := newJobService(t)
	register(t, h, "a", 4)
	countSpec := narrowJob(t, scheduler.ActionCount, 2)
	collectSpec := narrowJob(t, scheduler.ActionCollect, 2)
	count := startJob(t, jobs, context.Background(), countSpec)
	collect := startJob(t, jobs, context.Background(), collectSpec)
	seenJobs := make(map[plan.JobID]scheduler.ActionKind)
	for completed := 0; completed < 4; {
		assigned := awaitAssignments(t, h, "a", 4)
		for _, a := range assigned {
			seenJobs[a.JobID] = a.Task.FinalAction.Kind
			output := protocol.TaskOutput{Count: 3}
			wantPath := countSpec.Source.Path
			if a.Task.FinalAction.Kind == scheduler.ActionCollect {
				wantPath = collectSpec.Source.Path
				output = protocol.TaskOutput{Records: []json.RawMessage{}}
			}
			if a.Task.Operations[0].RDD.Operator.SourcePath != wantPath {
				t.Fatal("concurrent job used another job's lineage")
			}
			acknowledge(t, h, successReport(a, output))
			completed++
		}
	}
	gotCount, gotCollect := waitJob(t, count), waitJob(t, collect)
	if len(seenJobs) != 2 || gotCount.err != nil || gotCount.result.Count != 6 || gotCollect.err != nil ||
		gotCollect.result.Records == nil || len(gotCollect.result.Records) != 0 {
		t.Fatalf("jobs = %v, count = %+v, collect = %+v", seenJobs, gotCount, gotCollect)
	}
}

func TestJobServiceRejectsPlansBeforeScheduling(t *testing.T) {
	for _, name := range []string{"invalid spec", "unknown function", "shuffle"} {
		t.Run(name, func(t *testing.T) {
			jobs, tasks, h := newJobService(t)
			register(t, h, "a", 4)
			spec := narrowJob(t, scheduler.ActionCount, 4)
			want := "partition count"
			switch name {
			case "invalid spec":
				spec.Source.NumPartitions = 0
			case "unknown function":
				spec.Transformations[0].FunctionID = "missing"
				want = "missing"
			case "shuffle":
				spec.Transformations = []jobspec.TransformationSpec{
					{Kind: plan.OpMapToPair, FunctionID: "word_pair"},
					{Kind: plan.OpReduceByKey, FunctionID: "sum_int", NumPartitions: 2},
				}
				want = "shuffle execution is not implemented"
			}
			got := waitJob(t, startJob(t, jobs, context.Background(), spec))
			if got.err == nil || !strings.Contains(got.err.Error(), want) {
				t.Fatalf("error = %v, want %q", got.err, want)
			}
			select {
			case set := <-tasks.sets:
				t.Fatalf("rejected job scheduled tasks: %+v", set)
			default:
			}
			if assigned := offer(t, h, "a", 4); len(assigned) != 0 {
				t.Fatalf("rejected job assigned work: %+v", assigned)
			}
		})
	}
}

func TestJobServiceStopsJobsAndIgnoresLateReports(t *testing.T) {
	for _, reason := range []string{"failure", "cancel", "close"} {
		t.Run(reason, func(t *testing.T) {
			jobs, tasks, h := newJobService(t)
			register(t, h, "a", 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startJob(t, jobs, ctx, narrowJob(t, scheduler.ActionCount, 4))
			assigned := awaitAssignments(t, h, "a", 2)
			switch reason {
			case "failure":
				acknowledge(t, h, failureReport(successReport(assigned[0], protocol.TaskOutput{})))
			case "cancel":
				cancel()
			case "close":
				jobs.Close()
			}
			got := waitJob(t, done)
			if got.err == nil || (reason == "cancel" && !errors.Is(got.err, context.Canceled)) ||
				(reason == "failure" && !strings.Contains(got.err.Error(), "partition read failed")) ||
				(reason == "close" && !errors.Is(got.err, scheduler.ErrSchedulerClosed)) {
				t.Fatalf("stopped job returned %v", got.err)
			}
			for _, a := range assigned {
				acknowledge(t, h, successReport(a, protocol.TaskOutput{Count: 1000}))
			}
			reserved(t, tasks.FIFOTaskScheduler, 0)
			if next := offer(t, h, "a", 2); len(next) != 0 {
				t.Fatalf("stopped job assigned its pending partitions: %+v", next)
			}
			if reason == "close" {
				_, err := jobs.Submit(context.Background(), narrowJob(t, scheduler.ActionCount, 1))
				if !errors.Is(err, scheduler.ErrSchedulerClosed) {
					t.Fatalf("submit after Close = %v", err)
				}
				// Closing jobs leaves the caller's worker/placement scheduler open.
				register(t, h, "b", 1)
				return
			}
			// Old reports cannot contribute to a later job, even when logical IDs repeat.
			next := startJob(t, jobs, context.Background(), narrowJob(t, scheduler.ActionCount, 1))
			newAssignment := awaitAssignments(t, h, "a", 2)[0]
			for _, a := range assigned {
				acknowledge(t, h, successReport(a, protocol.TaskOutput{Count: 1000}))
			}
			assertJobPending(t, next)
			acknowledge(t, h, successReport(newAssignment, protocol.TaskOutput{Count: 7}))
			if got := waitJob(t, next); got.err != nil || got.result.Count != 7 {
				t.Fatalf("later job = %+v", got)
			}
		})
	}
}

func TestJobServiceRequiresDependencies(t *testing.T) {
	tasks := scheduler.NewFIFOTaskScheduler()
	defer tasks.Close()
	if _, err := coordinator.NewJobService(nil, tasks); err == nil {
		t.Fatal("nil registry accepted")
	}
	if _, err := coordinator.NewJobService(executor.NewFunctionRegistry(), nil); err == nil {
		t.Fatal("nil task scheduler accepted")
	}
}
