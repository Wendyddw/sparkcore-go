package coordinator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func offer(t *testing.T, h http.Handler, id plan.WorkerID, free int, running ...plan.TaskAttemptID) []protocol.TaskAssignment {
	t.Helper()
	if running == nil {
		running = []plan.TaskAttemptID{}
	}
	body, err := json.Marshal(protocol.HeartbeatRequest{WorkerID: id, FreeSlots: free, RunningAttemptIDs: running})
	if err != nil {
		t.Fatal(err)
	}
	w := request(h, http.MethodPost, protocol.HeartbeatPath, string(body))
	return response[protocol.HeartbeatResponse](t, w, http.StatusOK).Assignments
}

type ignoreReports struct{}

func (ignoreReports) TaskSucceeded(scheduler.TaskAttemptSuccess) {}
func (ignoreReports) TaskFailed(scheduler.TaskAttemptFailure)    {}

func submitTasks(t *testing.T, tasks *scheduler.FIFOTaskScheduler, partitions int) scheduler.TaskSet {
	t.Helper()
	set := scheduler.TaskSet{JobID: 11, StageID: 7, StageAttemptID: 9}
	for partition := partitions - 1; partition >= 0; partition-- {
		set.Tasks = append(set.Tasks, scheduler.Task{
			ID: plan.TaskID(1<<53 + partition), StageID: set.StageID, StageKind: scheduler.StageResult,
			PartitionID: plan.PartitionID(partition), NumPartitions: partitions,
			Operations: []scheduler.StageOperation{
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 0, Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "/missing/input.txt"}}},
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "normalize"}}},
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 2, Operator: plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "non_empty"}}},
			},
			FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCount, TargetRDD: 2},
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tasks.ScheduleTaskSet(ctx, set, ignoreReports{}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("unfinished submission returned %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("task-set submission did not stop")
		}
	})
	return set
}

func awaitAssignments(t *testing.T, h http.Handler, id plan.WorkerID, free int) []protocol.TaskAssignment {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if assignments := offer(t, h, id, free); len(assignments) != 0 {
			return assignments
		}
		runtime.Gosched()
	}
	t.Fatal("queued tasks were not assigned")
	return nil
}

func TestHeartbeatReturnsEmptyArrayAndRecordsStatus(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	got := offer(t, server.Handler, "a", 2)
	if got == nil || len(got) != 0 {
		t.Fatalf("idle assignments = %#v, want empty array", got)
	}
	worker, err := tasks.Worker("a")
	if err != nil || worker.ReportedFreeSlots != 2 || worker.ReservedSlots != 0 || worker.LastHeartbeat.IsZero() {
		t.Fatalf("heartbeat snapshot = %#v, %v", worker, err)
	}
}

func TestHeartbeatPreservesAssignmentPipelineAndReservations(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	register(t, server.Handler, "b", 1)
	set := submitTasks(t, tasks, 4)
	assignments := awaitAssignments(t, server.Handler, "a", 2)
	if len(assignments) != 2 {
		t.Fatalf("assignments = %d, want 2", len(assignments))
	}
	for i, assignment := range assignments {
		wantTask := set.Tasks[len(set.Tasks)-1-i]
		if assignment.JobID != set.JobID || assignment.StageID != set.StageID || assignment.WorkerID != "a" ||
			assignment.Attempt.StageAttemptID != set.StageAttemptID || assignment.Attempt.TaskID != wantTask.ID ||
			assignment.Attempt.ID != plan.TaskAttemptID(i) || !reflect.DeepEqual(assignment.Task, wantTask) {
			t.Fatalf("assignment = %#v, want task %#v", assignment, wantTask)
		}
	}
	before, _ := tasks.Worker("a")
	register(t, server.Handler, "a", 2)
	checkError(t, request(server.Handler, "POST", protocol.RegisterWorkerPath, `{"worker_id":"a","total_slots":3}`), 409, protocol.CodeConflict)
	after, _ := tasks.Worker("a")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("repeat registration changed active reservations")
	}
	if got := offer(t, server.Handler, "a", 2); len(got) != 0 {
		t.Fatal("repeated offer reused assignments in transit")
	}
	if got := offer(t, server.Handler, "a", 0, assignments[0].Attempt.ID, assignments[1].Attempt.ID); len(got) != 0 {
		t.Fatal("full worker received assignments")
	}
	w := request(server.Handler, "POST", protocol.HeartbeatPath,
		fmt.Sprintf(`{"worker_id":"b","free_slots":0,"running_attempt_ids":[%d]}`, assignments[0].Attempt.ID))
	checkError(t, w, http.StatusBadRequest, protocol.CodeInvalidRequest)
	if worker, _ := tasks.Worker("b"); !worker.LastHeartbeat.IsZero() {
		t.Fatal("foreign attempt changed worker heartbeat")
	}
	if got := offer(t, server.Handler, "b", 1); len(got) != 1 || got[0].Task.PartitionID != 2 {
		t.Fatalf("second worker assignments = %#v", got)
	}
}

func TestConcurrentRegistrationsAndHeartbeatsRespectCapacity(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "prime", 1)
	submitTasks(t, tasks, 6)
	// Receiving the first partition establishes that the set is queued.
	first := awaitAssignments(t, server.Handler, "prime", 1)
	if len(first) != 1 || first[0].Task.PartitionID != 0 {
		t.Fatalf("first assignment = %#v", first)
	}
	var wg sync.WaitGroup
	assignments := make(chan protocol.TaskAssignment, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := plan.WorkerID(fmt.Sprintf("worker-%d", i%2))
			register(t, server.Handler, id, 2)
			for _, assignment := range offer(t, server.Handler, id, 2) {
				assignments <- assignment
			}
		}(i)
	}
	wg.Wait()
	close(assignments)
	seen := make(map[plan.PartitionID]bool)
	for assignment := range assignments {
		partition := assignment.Task.PartitionID
		if seen[partition] || partition < 1 || partition > 4 {
			t.Fatalf("duplicate or unexpected partition: %d", partition)
		}
		seen[partition] = true
	}
	if len(seen) != 4 {
		t.Fatalf("assigned %d partitions, want 4", len(seen))
	}
	for _, id := range []plan.WorkerID{"worker-0", "worker-1"} {
		worker, err := tasks.Worker(id)
		if err != nil || worker.ReservedSlots != 2 {
			t.Fatalf("worker %s: %#v, %v", id, worker, err)
		}
	}
}
