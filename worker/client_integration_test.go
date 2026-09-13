package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

func coordinatorClient(t *testing.T) (*scheduler.FIFOTaskScheduler, *worker.Client) {
	t.Helper()
	tasks := scheduler.NewFIFOTaskScheduler()
	t.Cleanup(tasks.Close)
	server, err := coordinator.NewServer(tasks, nil, coordinator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)
	return tasks, newClient(t, ts.URL, worker.ClientConfig{})
}

type clientObserver struct {
	successes chan scheduler.TaskAttemptSuccess
	failures  chan scheduler.TaskAttemptFailure
}

func (o clientObserver) TaskSucceeded(r scheduler.TaskAttemptSuccess) { o.successes <- r }
func (o clientObserver) TaskFailed(r scheduler.TaskAttemptFailure)    { o.failures <- r }

func TestClientCompletesCoordinatorTaskReports(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			tasks, client := coordinatorClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := client.RegisterWorker(ctx, protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 1}); err != nil {
				t.Fatal(err)
			}
			offer := protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}}
			idle, err := client.Heartbeat(ctx, offer)
			if err != nil || idle.Assignments == nil || len(idle.Assignments) != 0 {
				t.Fatalf("idle heartbeat = %#v, err=%v", idle, err)
			}
			fixture := assignment()
			observer := clientObserver{make(chan scheduler.TaskAttemptSuccess, 2), make(chan scheduler.TaskAttemptFailure, 2)}
			done := make(chan error, 1)
			go func() {
				done <- tasks.ScheduleTaskSet(ctx, scheduler.TaskSet{JobID: fixture.JobID, StageID: fixture.StageID,
					StageAttemptID: fixture.Attempt.StageAttemptID, Tasks: []scheduler.Task{fixture.Task}}, observer)
			}()
			var assigned protocol.TaskAssignment
			for {
				response, err := client.Heartbeat(ctx, offer)
				if err != nil {
					t.Fatal(err)
				}
				if len(response.Assignments) != 0 {
					assigned = response.Assignments[0]
					break
				}
				runtime.Gosched()
			}
			for range 2 {
				var ack protocol.TaskReportResponse
				var err error
				if failure {
					ack, err = client.ReportFailure(ctx, failureRequest(assigned))
				} else {
					ack, err = client.ReportSuccess(ctx, successRequest(assigned))
				}
				if err != nil || !ack.Acknowledged {
					t.Fatalf("report acknowledgment = %#v, %v", ack, err)
				}
			}
			select {
			case err := <-done:
				if (err != nil) != failure {
					t.Fatalf("completion = %v, failure=%v", err, failure)
				}
			case <-ctx.Done():
				t.Fatal("report did not complete task set")
			}
			if failure {
				if len(observer.failures) != 1 || len(observer.successes) != 0 {
					t.Fatal("failure did not arrive exactly once")
				}
				got := <-observer.failures
				if got.Attempt != assigned.Attempt || got.Error != failureRequest(assigned).Error {
					t.Fatalf("failure callback = %#v", got)
				}
			} else {
				if len(observer.successes) != 1 || len(observer.failures) != 0 {
					t.Fatal("success did not arrive exactly once")
				}
				got := <-observer.successes
				raw, ok := got.Output.Records[0].(json.RawMessage)
				if !ok || string(raw) != string(successRequest(assigned).Output.Records[0]) || got.Attempt != assigned.Attempt {
					t.Fatalf("success callback lost payload or identity: %#v", got)
				}
			}
			if snapshot, err := tasks.Worker("a"); err != nil || snapshot.ReservedSlots != 0 {
				t.Fatalf("terminal report left reservations: %#v, %v", snapshot, err)
			}
		})
	}
}

func TestClientPreservesCoordinatorHTTPErrors(t *testing.T) {
	tasks, client := coordinatorClient(t)
	ctx := context.Background()
	if _, err := client.RegisterWorker(ctx, protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 1}); err != nil {
		t.Fatal(err)
	}
	assertError := func(err error, status int, code protocol.ErrorCode) {
		t.Helper()
		var got *worker.HTTPError
		if !errors.As(err, &got) || got.StatusCode != status || got.Code != code || got.Message == "" {
			t.Fatalf("HTTP error = %v, want %d %s", err, status, code)
		}
	}
	_, err := client.RegisterWorker(ctx, protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 2})
	assertError(err, 409, protocol.CodeConflict)
	_, err = client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: "missing", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}})
	assertError(err, 404, protocol.CodeNotFound)
	_, err = client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 2, RunningAttemptIDs: []plan.TaskAttemptID{}})
	assertError(err, 400, protocol.CodeInvalidRequest)
	_, err = client.ReportFailure(ctx, failureRequest(assignment()))
	assertError(err, 404, protocol.CodeNotFound)
	tasks.Close()
	_, err = client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}})
	assertError(err, 503, protocol.CodeUnavailable)
}
