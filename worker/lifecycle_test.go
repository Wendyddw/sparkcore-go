package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
)

func TestRuntimeLogsExecutionAndReportingSeparately(t *testing.T) {
	for _, outcome := range []string{"success", "task failure", "report failure"} {
		t.Run(outcome, func(t *testing.T) {
			var logs bytes.Buffer
			reported := make(chan struct{}, 1)
			sent := false
			ack := func() (protocol.TaskReportResponse, error) {
				reported <- struct{}{}
				if outcome == "report failure" {
					return protocol.TaskReportResponse{}, errors.New("report unavailable")
				}
				return protocol.TaskReportResponse{Acknowledged: true}, nil
			}
			client := runtimeClient{
				heartbeat: func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
					if sent {
						return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
					}
					sent = true
					return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{runtimeAssignment(0)}}, nil
				},
				success: func(context.Context, protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) { return ack() },
				failure: func(context.Context, protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) { return ack() },
			}
			config := runtimeConfig(1)
			config.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			config.Sources = recordsSource("one")
			if outcome == "task failure" {
				config.Sources = sourceFunc(func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error) {
					return nil, errors.New("source unavailable")
				})
			}
			run := startRuntime(t, newRuntime(t, client, config))
			receive(t, reported)
			if outcome == "report failure" {
				run.wait(t)
			} else {
				run.cancel()
				run.wait(t)
			}
			decoder := json.NewDecoder(&logs)
			decoder.UseNumber()
			events := make(map[string]map[string]any)
			for decoder.More() {
				var event map[string]any
				if err := decoder.Decode(&event); err != nil {
					t.Fatal(err)
				}
				name := event["msg"].(string)
				if events[name] != nil {
					t.Fatalf("duplicate event %s", name)
				}
				events[name] = event
			}
			if len(events) != 3 || events["worker_registered"] == nil || events["task_started"] == nil || events["task_finished"] == nil {
				t.Fatalf("events = %v", events)
			}
			start, finish := events["task_started"], events["task_finished"]
			for _, key := range []string{"job_id", "stage_id", "stage_attempt_id", "task_id", "attempt_id", "partition_id", "worker_id"} {
				if start[key] == nil || start[key] != finish[key] {
					t.Fatalf("missing/mismatched %s: %v %v", key, start, finish)
				}
			}
			if finish["execution_succeeded"] != (outcome != "task failure") || finish["report_acknowledged"] != (outcome != "report failure") {
				t.Fatalf("outcome=%s event=%v", outcome, finish)
			}
		})
	}
}
