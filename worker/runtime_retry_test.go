package worker_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestRuntimeAcceptsRetryWhileOldReportAwaitsAcknowledgment(t *testing.T) {
	first := runtimeAssignment(0)
	retry := first
	retry.Attempt.ID++
	var accepted atomic.Bool
	var opens atomic.Int32
	releaseAck := make(chan struct{})
	defer close(releaseAck)
	offers := make(chan protocol.HeartbeatRequest, 1)
	successes := make(chan protocol.TaskSuccessRequest, 1)
	sent := 0 // Only the runtime's heartbeat loop accesses this counter.
	client := runtimeClient{
		heartbeat: func(_ context.Context, offer protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
			batch := []protocol.TaskAssignment{}
			if sent == 0 {
				sent++
				batch = append(batch, first)
			} else if sent == 1 && accepted.Load() {
				sent++
				offers <- offer
				batch = append(batch, retry)
			}
			return protocol.HeartbeatResponse{Assignments: batch}, nil
		},
		failure: func(ctx context.Context, report protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) {
			if report.Attempt != first.Attempt || report.Kind != scheduler.FailureExecution {
				t.Errorf("unexpected failure: %+v", report)
			}
			accepted.Store(true) // Coordinator accepted it, but its response is delayed.
			select {
			case <-releaseAck:
			case <-ctx.Done():
				return protocol.TaskReportResponse{}, ctx.Err()
			}
			return protocol.TaskReportResponse{Acknowledged: true}, nil
		},
		success: func(_ context.Context, report protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
			successes <- report
			return protocol.TaskReportResponse{Acknowledged: true}, nil
		},
	}
	config := runtimeConfig(2)
	config.Sources = sourceFunc(func(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
		if opens.Add(1) == 1 {
			return nil, errors.New("fail once")
		}
		return recordsSource("record").Open(ctx, path, p, n)
	})
	run := startRuntime(t, newRuntime(t, client, config))
	offer := receive(t, offers)
	if offer.FreeSlots != 1 || !reflect.DeepEqual(offer.RunningAttemptIDs, []plan.TaskAttemptID{first.Attempt.ID}) {
		t.Fatalf("old attempt released early: %+v", offer)
	}
	result := receive(t, successes)
	if result.Attempt != retry.Attempt || result.Output.Count != 1 || opens.Load() != 2 {
		t.Fatalf("retry result = %+v; executions = %d", result, opens.Load())
	}
	// Deferred release lets Run join the old report before the test cleanup waits.
	run.cancel()
}
