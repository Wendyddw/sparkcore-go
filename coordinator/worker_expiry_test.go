package coordinator_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestJobDeadlineStillAppliesAfterAllWorkersAreLost(t *testing.T) {
	jobs, tasks, _ := newJobService(t)
	ts := liveJobServer(t, tasks, jobs, coordinator.Config{JobTimeout: 500 * time.Millisecond})
	t.Cleanup(jobs.Close)
	register(t, ts.Config.Handler, "a", 1)
	done := startHTTPJob(t, ts, context.Background(), narrowJob(t, scheduler.ActionCount, 2))
	assigned := awaitAssignments(t, ts.Config.Handler, "a", 1)[0]
	before, _ := tasks.Worker("a")
	if _, err := tasks.ExpireWorkers(before.LastHeartbeat.Add(10*time.Second), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	got := httpJobResponse[protocol.ErrorResponse](t, done, http.StatusGatewayTimeout)
	if got.Code != protocol.CodeJobFailed {
		t.Fatal(got)
	}
	acknowledge(t, ts.Config.Handler, successReport(assigned, protocol.TaskOutput{Count: 999}))
	register(t, ts.Config.Handler, "b", 1)
	if more := offer(t, ts.Config.Handler, "b", 1); len(more) != 0 {
		t.Fatal("expired job was revived by a new worker")
	}
}

func TestLostWorkerHTTPFencingAndLateReports(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 1)
	run := startReportRun(t, tasks, 1, scheduler.ActionCount)
	old := awaitAssignments(t, server.Handler, "a", 1)[0]
	before, _ := tasks.Worker("a")
	if lost, err := tasks.ExpireWorkers(before.LastHeartbeat.Add(10*time.Second), 10*time.Second); err != nil || len(lost) != 1 {
		t.Fatalf("expiry = %v, %v", lost, err)
	}
	checkError(t, request(server.Handler, http.MethodPost, protocol.RegisterWorkerPath, `{"worker_id":"a","total_slots":1}`), 409, protocol.CodeConflict)
	checkError(t, request(server.Handler, http.MethodPost, protocol.HeartbeatPath, `{"worker_id":"a","free_slots":1,"running_attempt_ids":[]}`), 409, protocol.CodeConflict)
	reserved(t, tasks, 0)
	register(t, server.Handler, "b", 1)
	retry := awaitAssignments(t, server.Handler, "b", 1)[0]
	if retry.Attempt.ID == old.Attempt.ID || retry.Attempt.TaskID != old.Attempt.TaskID {
		t.Fatal("replacement identity is incorrect")
	}
	late := successReport(old, protocol.TaskOutput{Count: 999})
	acknowledge(t, server.Handler, late)
	acknowledge(t, server.Handler, failureReport(late))
	bad := late
	bad.JobID++
	checkError(t, postReport(t, server.Handler, bad), 409, protocol.CodeConflict)
	bad = late
	bad.Attempt.ID = 999
	checkError(t, postReport(t, server.Handler, bad), 404, protocol.CodeNotFound)
	claimedLoss := failureReport(successReport(retry, protocol.TaskOutput{}))
	claimedLoss.Kind = scheduler.FailureWorkerLost
	checkError(t, postReport(t, server.Handler, claimedLoss), 400, protocol.CodeInvalidRequest)
	if live, _ := tasks.Worker("b"); live.ReservedSlots != 1 {
		t.Fatal("old report released replacement slot")
	}
	acknowledge(t, server.Handler, successReport(retry, protocol.TaskOutput{Count: 7}))
	if err := run.wait(t); err != nil {
		t.Fatal(err)
	}
	if len(run.successes) != 1 || len(run.failures) != 0 {
		t.Fatal("loss leaked an intermediate failure or accepted stale output")
	}
	if got := <-run.successes; got.Output.Count != 7 || got.Attempt != retry.Attempt {
		t.Fatal(got)
	}
	if lost, _ := tasks.Worker("a"); lost.Status != scheduler.WorkerLost || lost.LastHeartbeat != before.LastHeartbeat {
		t.Fatal("old report revived lost worker")
	}
}
