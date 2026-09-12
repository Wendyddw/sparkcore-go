package coordinator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

type reportObserver struct {
	successes chan scheduler.TaskAttemptSuccess
	failures  chan scheduler.TaskAttemptFailure
}

func (o *reportObserver) TaskSucceeded(r scheduler.TaskAttemptSuccess) { o.successes <- r }
func (o *reportObserver) TaskFailed(r scheduler.TaskAttemptFailure)    { o.failures <- r }

type reportRun struct {
	*reportObserver
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startReportRun(t *testing.T, tasks *scheduler.FIFOTaskScheduler, partitions int, action scheduler.ActionKind) *reportRun {
	t.Helper()
	set := httpTaskSet(partitions)
	for i := range set.Tasks {
		set.Tasks[i].FinalAction.Kind = action
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &reportRun{reportObserver: &reportObserver{
		successes: make(chan scheduler.TaskAttemptSuccess, 64), failures: make(chan scheduler.TaskAttemptFailure, 64),
	}, cancel: cancel, done: make(chan struct{})}
	go func() {
		run.err = tasks.ScheduleTaskSet(ctx, set, run.reportObserver)
		close(run.done)
	}()
	t.Cleanup(func() { cancel(); run.wait(t) })
	return run
}

func (r *reportRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(3 * time.Second):
		t.Fatal("task set did not finish")
		return nil
	}
}

func successReport(a protocol.TaskAssignment, output protocol.TaskOutput) protocol.TaskSuccessRequest {
	return protocol.TaskSuccessRequest{JobID: a.JobID, StageID: a.StageID, Attempt: a.Attempt,
		PartitionID: a.Task.PartitionID, WorkerID: a.WorkerID, Output: output}
}

func failureReport(r protocol.TaskSuccessRequest) protocol.TaskFailureRequest {
	return protocol.TaskFailureRequest{JobID: r.JobID, StageID: r.StageID, Attempt: r.Attempt,
		PartitionID: r.PartitionID, WorkerID: r.WorkerID, Error: "partition read failed"}
}

func postReport(t *testing.T, h http.Handler, value any) *httptest.ResponseRecorder {
	t.Helper()
	path := protocol.TaskSuccessPath
	if _, failure := value.(protocol.TaskFailureRequest); failure {
		path = protocol.TaskFailurePath
	}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return request(h, http.MethodPost, path, string(body))
}

func acknowledge(t *testing.T, h http.Handler, value any) {
	t.Helper()
	response[protocol.TaskReportResponse](t, postReport(t, h, value), http.StatusOK)
}

func reserved(t *testing.T, tasks *scheduler.FIFOTaskScheduler, want int) {
	t.Helper()
	worker, err := tasks.Worker("a")
	if err != nil || worker.ReservedSlots != want {
		t.Fatalf("worker reservations = %d, want %d, err=%v", worker.ReservedSlots, want, err)
	}
}

func TestSuccessReportsPreserveOutputsAndReleaseEachSlotOnce(t *testing.T) {
	for _, action := range []scheduler.ActionKind{scheduler.ActionCount, scheduler.ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
			tasks, server := newService(t, coordinator.Config{})
			register(t, server.Handler, "a", 2)
			run := startReportRun(t, tasks, 3, action)
			assigned := awaitAssignments(t, server.Handler, "a", 2)
			outputs := []protocol.TaskOutput{{Count: 0}, {Count: 9007199254740993}, {Count: 17}}
			if action == scheduler.ActionCollect {
				outputs = []protocol.TaskOutput{
					{Records: []json.RawMessage{}},
					{Records: []json.RawMessage{json.RawMessage(`{"id":9007199254740993,"nested":[null,true]}`), json.RawMessage(`9223372036854775807`)}},
					{Records: []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`"last"`)}},
				}
			}
			first := successReport(assigned[1], outputs[1])
			acknowledge(t, server.Handler, first)
			reserved(t, tasks, 1)
			duplicate := first
			duplicate.Output = protocol.TaskOutput{Count: 5}
			acknowledge(t, server.Handler, duplicate)
			acknowledge(t, server.Handler, failureReport(first))
			reserved(t, tasks, 1)
			next := offer(t, server.Handler, "a", 1, assigned[0].Attempt.ID)
			if len(next) != 1 || next[0].Task.PartitionID != 2 {
				t.Fatalf("released slot assignments = %#v", next)
			}
			assigned = append(assigned, next[0])
			acknowledge(t, server.Handler, successReport(assigned[2], outputs[2]))
			acknowledge(t, server.Handler, successReport(assigned[0], outputs[0]))
			if err := run.wait(t); err != nil {
				t.Fatal(err)
			}
			reserved(t, tasks, 0)
			if len(run.successes) != 3 || len(run.failures) != 0 {
				t.Fatalf("callbacks: successes=%d failures=%d", len(run.successes), len(run.failures))
			}
			for range 3 {
				got := <-run.successes
				want := successReport(assigned[int(got.PartitionID)], outputs[int(got.PartitionID)])
				if got.JobID != want.JobID || got.StageID != want.StageID || got.Attempt != want.Attempt || got.WorkerID != want.WorkerID || got.Output.Count != want.Output.Count {
					t.Fatalf("callback identity/count = %#v, want %#v", got, want)
				}
				if len(got.Output.Records) != len(want.Output.Records) {
					t.Fatalf("record count = %d, want %d", len(got.Output.Records), len(want.Output.Records))
				}
				for i, record := range got.Output.Records {
					raw, ok := record.(json.RawMessage)
					if !ok || string(raw) != string(want.Output.Records[i]) {
						t.Fatalf("record lost raw JSON or precision: %#v", record)
					}
				}
			}
			// Retained attempt history also acknowledges reports after set completion.
			acknowledge(t, server.Handler, first)
			if len(run.successes) != 0 {
				t.Fatal("duplicate produced a callback after completion")
			}
		})
	}
}

func TestFailureReportsTerminateSetAndRetainSiblingReservations(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	run := startReportRun(t, tasks, 3, scheduler.ActionCount)
	assigned := awaitAssignments(t, server.Handler, "a", 2)
	failure := failureReport(successReport(assigned[0], protocol.TaskOutput{}))
	acknowledge(t, server.Handler, failure)
	if err := run.wait(t); err == nil || !strings.Contains(err.Error(), failure.Error) {
		t.Fatalf("task-set failure = %v", err)
	}
	reserved(t, tasks, 1)
	acknowledge(t, server.Handler, failure)
	acknowledge(t, server.Handler, successReport(assigned[0], protocol.TaskOutput{Count: 42}))
	reserved(t, tasks, 1)
	if got := offer(t, server.Handler, "a", 1, assigned[1].Attempt.ID); len(got) != 0 {
		t.Fatal("failed task set received more assignments")
	}
	acknowledge(t, server.Handler, successReport(assigned[1], protocol.TaskOutput{Count: 99}))
	acknowledge(t, server.Handler, failureReport(successReport(assigned[1], protocol.TaskOutput{})))
	reserved(t, tasks, 0)
	if len(run.failures) != 1 || len(run.successes) != 0 {
		t.Fatalf("callbacks: successes=%d failures=%d", len(run.successes), len(run.failures))
	}
	got := <-run.failures
	want := scheduler.TaskAttemptFailure{JobID: failure.JobID, StageID: failure.StageID, Attempt: failure.Attempt,
		PartitionID: failure.PartitionID, WorkerID: failure.WorkerID, Error: failure.Error}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failure = %#v, want %#v", got, want)
	}
}

func TestCanceledSetAcknowledgesLateReportsWithoutCallbacks(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	run := startReportRun(t, tasks, 2, scheduler.ActionCount)
	assigned := awaitAssignments(t, server.Handler, "a", 2)
	run.cancel()
	if err := run.wait(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled set = %v", err)
	}
	reserved(t, tasks, 2)
	acknowledge(t, server.Handler, successReport(assigned[0], protocol.TaskOutput{Count: 1}))
	acknowledge(t, server.Handler, failureReport(successReport(assigned[1], protocol.TaskOutput{})))
	reserved(t, tasks, 0)
	if len(run.successes) != 0 || len(run.failures) != 0 {
		t.Fatal("obsolete reports produced callbacks")
	}
}

func TestReportIdentityErrorsDoNotChangeSchedulerState(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 1)
	register(t, server.Handler, "b", 1)
	run := startReportRun(t, tasks, 1, scheduler.ActionCount)
	assigned := awaitAssignments(t, server.Handler, "a", 1)[0]
	valid := successReport(assigned, protocol.TaskOutput{Count: 1})
	for _, test := range []struct {
		name   string
		mutate func(*protocol.TaskSuccessRequest)
		status int
	}{
		{"unknown worker", func(r *protocol.TaskSuccessRequest) { r.WorkerID = "missing" }, 404},
		{"unknown attempt", func(r *protocol.TaskSuccessRequest) { r.Attempt.ID++ }, 404},
		{"wrong worker", func(r *protocol.TaskSuccessRequest) { r.WorkerID = "b" }, 409},
		{"wrong job", func(r *protocol.TaskSuccessRequest) { r.JobID++ }, 409},
		{"wrong stage", func(r *protocol.TaskSuccessRequest) { r.StageID++ }, 409},
		{"wrong stage attempt", func(r *protocol.TaskSuccessRequest) { r.Attempt.StageAttemptID++ }, 409},
		{"wrong task", func(r *protocol.TaskSuccessRequest) { r.Attempt.TaskID++ }, 409},
		{"wrong partition", func(r *protocol.TaskSuccessRequest) { r.PartitionID++ }, 409},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := valid
			test.mutate(&invalid)
			code := protocol.CodeConflict
			if test.status == 404 {
				code = protocol.CodeNotFound
			}
			for _, report := range []any{invalid, failureReport(invalid)} {
				checkError(t, postReport(t, server.Handler, report), test.status, code)
			}
			reserved(t, tasks, 1)
		})
	}
	acknowledge(t, server.Handler, valid)
	if err := run.wait(t); err != nil {
		t.Fatal(err)
	}
	if len(run.successes) != 1 || len(run.failures) != 0 {
		t.Fatal("invalid reports produced callbacks")
	}
}

func TestReportEndpointsRejectInvalidHTTPRequests(t *testing.T) {
	for _, path := range []string{protocol.TaskSuccessPath, protocol.TaskFailurePath} {
		t.Run(path, func(t *testing.T) {
			tasks, server := newService(t, coordinator.Config{})
			register(t, server.Handler, "a", 1)
			startReportRun(t, tasks, 1, scheduler.ActionCount)
			assigned := awaitAssignments(t, server.Handler, "a", 1)[0]
			success := successReport(assigned, protocol.TaskOutput{Count: 1})
			var value any = success
			if path == protocol.TaskFailurePath {
				value = failureReport(success)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			body := string(encoded)
			w := request(server.Handler, "GET", path, body)
			checkError(t, w, 405, protocol.CodeMethodNotAllowed)
			if w.Header().Get("Allow") != "POST" {
				t.Fatal("missing Allow header")
			}
			for _, invalid := range []string{
				"{", body + "{}", strings.Replace(body, `"worker_id":`, `"extra":0,"worker_id":`, 1),
				strings.Replace(body, `"partition_id":0`, `"partition_id":null`, 1),
			} {
				checkError(t, request(server.Handler, "POST", path, invalid), 400, protocol.CodeInvalidRequest)
			}
			invalid := strings.Replace(body, `"count":1`, `"count":-1`, 1)
			if path == protocol.TaskFailurePath {
				invalid = strings.Replace(body, `"partition read failed"`, `""`, 1)
			}
			checkError(t, request(server.Handler, "POST", path, invalid), 400, protocol.CodeInvalidRequest)
			limited, err := coordinator.NewServer(tasks, coordinator.Config{MaxRequestBytes: int64(len(body))})
			if err != nil {
				t.Fatal(err)
			}
			checkError(t, request(limited.Handler, "POST", path, body+" "), 413, protocol.CodeRequestTooLarge)
			reserved(t, tasks, 1)
			response[protocol.TaskReportResponse](t, request(limited.Handler, "POST", path, body), 200)
			reserved(t, tasks, 0)
			tasks.Close()
			checkError(t, request(server.Handler, "POST", path, body), 503, protocol.CodeUnavailable)
		})
	}
}

func TestConcurrentReportsAndHeartbeatsReleaseReservationsOnce(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			tasks, server := newService(t, coordinator.Config{})
			register(t, server.Handler, "a", 1)
			run := startReportRun(t, tasks, 1, scheduler.ActionCount)
			assigned := awaitAssignments(t, server.Handler, "a", 1)[0]
			success := successReport(assigned, protocol.TaskOutput{Count: 7})
			var report any = success
			if failure {
				report = failureReport(success)
			}
			var wg sync.WaitGroup
			for range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					acknowledge(t, server.Handler, report)
					if got := offer(t, server.Handler, "a", 0, assigned.Attempt.ID); len(got) != 0 {
						t.Error("duplicate report or delayed heartbeat created work")
					}
				}()
			}
			wg.Wait()
			if err := run.wait(t); (err != nil) != failure {
				t.Fatalf("completion error = %v, failure=%v", err, failure)
			}
			reserved(t, tasks, 0)
			if failure && (len(run.failures) != 1 || len(run.successes) != 0) ||
				!failure && (len(run.successes) != 1 || len(run.failures) != 0) {
				t.Fatalf("callbacks: successes=%d failures=%d", len(run.successes), len(run.failures))
			}
		})
	}
}
