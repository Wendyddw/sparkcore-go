package worker

import (
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

type executionRecorder struct{ received scheduler.TaskExecution }

func (r *executionRecorder) RunTask(_ context.Context, execution scheduler.TaskExecution) (scheduler.TaskOutput, error) {
	r.received = execution
	return scheduler.TaskOutput{Count: 2}, execution.Validate()
}

type identityReportClient struct {
	CoordinatorClient
	received protocol.TaskSuccessRequest
}

func (c *identityReportClient) ReportSuccess(_ context.Context, report protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
	c.received = report
	return protocol.TaskReportResponse{Acknowledged: true}, nil
}

func TestWorkerPassesAssignedIdentityToRunnerAndReport(t *testing.T) {
	a := protocol.TaskAssignment{
		RunID: "0123456789abcdef0123456789abcdef", JobID: 7, StageID: 3, WorkerID: "worker-a",
		Attempt: scheduler.TaskAttemptIdentity{ID: 19, TaskID: 11, StageAttemptID: 5},
		Task: scheduler.Task{ID: 11, StageID: 3, PartitionID: 2,
			FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCount}},
	}
	runner, client := &executionRecorder{}, &identityReportClient{}
	w := &Runtime{runner: runner, client: client, config: RuntimeConfig{RequestTimeout: time.Second},
		logger: slog.New(slog.DiscardHandler)}
	if err := w.executeAndReport(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	want := scheduler.TaskExecution{RunID: a.RunID, JobID: a.JobID, Task: a.Task, Attempt: a.Attempt, WorkerID: a.WorkerID}
	if !reflect.DeepEqual(runner.received, want) {
		t.Fatalf("worker changed execution identity: %+v", runner.received)
	}
	r := client.received
	if r.Attempt != a.Attempt || r.JobID != a.JobID || r.StageID != a.StageID ||
		r.WorkerID != a.WorkerID || r.PartitionID != a.Task.PartitionID || r.Output.Count != 2 {
		t.Fatalf("report identity differs from executed attempt: %+v", r)
	}
}
