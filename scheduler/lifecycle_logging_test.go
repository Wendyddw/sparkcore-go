package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

// Read only after the owning scheduling goroutines have joined.
func logEvents(t *testing.T, logs *bytes.Buffer) map[string][]map[string]any {
	t.Helper()
	events := make(map[string][]map[string]any)
	decoder := json.NewDecoder(logs)
	for decoder.More() {
		var event map[string]any
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		name, _ := event["msg"].(string)
		events[name] = append(events[name], event)
	}
	return events
}

func TestRetryLogsDistinguishAttemptFailureAndExhaustion(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhausted", true: "permanent"}[permanent], func(t *testing.T) {
			var logs bytes.Buffer
			s := NewFIFOTaskScheduler(WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))), WithMaxTaskAttempts(2))
			defer s.Close()
			if err := s.RegisterWorker("a", 1); err != nil {
				t.Fatal(err)
			}
			observer, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
			count := 2
			if permanent {
				count = 1
			}
			for range count {
				a := fifoOffer(t, s, "a", 1)[0]
				report := failureFor(a)
				if permanent {
					report.Kind = FailurePermanent
				}
				if err := s.ReportFailure(report); err != nil {
					t.Fatal(err)
				}
				if err := s.ReportFailure(report); err != nil {
					t.Fatal(err)
				}
			}
			if err := fifoWait(t, done); err == nil {
				t.Fatal("expected failure")
			}
			if len(observer.failures) != 1 {
				t.Fatal("retry leaked to observer")
			}
			events := logEvents(t, &logs)
			if len(events["task_attempt_failed"]) != count || len(events["task_requeued"]) != count-1 || len(events["task_failed"]) != 1 {
				t.Fatalf("events = %v", events)
			}
			final := events["task_failed"][0]
			if final["budget_exhausted"] != !permanent || final["attempts"] != float64(count) || final["max_task_attempts"] != float64(2) {
				t.Fatalf("terminal event = %v", final)
			}
		})
	}
}

func TestShuffleAcceptanceAndRecoveryLogs(t *testing.T) {
	var logs bytes.Buffer
	dag, submissions, done, _ := startRecoveryJob(t, WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))))
	run := NewRunID()
	maps := dagReceive(t, submissions)
	completeMaps(maps, run)
	maps.observer.TaskSucceeded(mapSuccess(maps.set, 0, run)) // Duplicate acceptance must not be logged.
	result := dagReceive(t, submissions)
	result.observer.TaskFailed(inputFailure(result, 0))
	replacement := dagReceive(t, submissions)
	completeMaps(replacement, run)
	reducers := dagReceive(t, submissions)
	for p := range reducers.set.Tasks {
		reducers.observer.TaskSucceeded(successFor(reducers.set, p, TaskOutput{Count: 1}))
	}
	if got := dagReceive(t, done); got.err != nil {
		t.Fatal(got.err)
	}
	dag.Close()
	events := logEvents(t, &logs)
	if len(events["shuffle_output_accepted"]) != 4 || len(events["shuffle_recovery_started"]) != 1 {
		t.Fatalf("events = %v", events)
	}
	for _, event := range events["shuffle_output_accepted"] {
		if event["run_id"] != run || event["stage_attempt_id"] == nil || event["attempt_id"] == nil || event["shuffle_id"] == nil {
			t.Fatalf("missing identity: %v", event)
		}
	}
}
