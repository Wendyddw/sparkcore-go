package executor

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestLocalTaskSchedulerAssignsUniqueMonotonicAttempts(t *testing.T) {
	runner := &immediateTaskRunner{}
	observer := &recordingTaskSetObserver{}
	taskSet := scheduler.TaskSet{
		JobID:          4,
		StageID:        2,
		StageAttemptID: 3,
		Tasks: []scheduler.Task{
			{ID: 10, StageID: 2, PartitionID: 0},
			{ID: 11, StageID: 2, PartitionID: 1},
			{ID: 12, StageID: 2, PartitionID: 2},
		},
	}

	err := NewLocalTaskScheduler(runner).ScheduleTaskSet(context.Background(), taskSet, observer)
	if err != nil {
		t.Fatalf("ScheduleTaskSet() error = %v", err)
	}
	successes, failures := observer.reports()
	if len(failures) != 0 || len(successes) != 3 {
		t.Fatalf("reports = %d successes, %d failures; want 3, 0", len(successes), len(failures))
	}
	sort.Slice(successes, func(i, j int) bool { return successes[i].Attempt.ID < successes[j].Attempt.ID })
	for i, report := range successes {
		if report.Attempt.ID != plan.TaskAttemptID(i) {
			t.Errorf("attempt %d ID = %d, want %d", i, report.Attempt.ID, i)
		}
		if report.Attempt.TaskID != taskSet.Tasks[i].ID {
			t.Errorf("attempt %d task ID = %d, want %d", i, report.Attempt.TaskID, taskSet.Tasks[i].ID)
		}
		if report.Attempt.StageAttemptID != taskSet.StageAttemptID {
			t.Errorf("attempt %d stage attempt = %d, want %d", i, report.Attempt.StageAttemptID, taskSet.StageAttemptID)
		}
		if report.WorkerID != localWorkerID {
			t.Errorf("attempt %d worker = %q, want %q", i, report.WorkerID, localWorkerID)
		}
	}
}

func TestLocalTaskSchedulerReportsTaskFailure(t *testing.T) {
	runner := &immediateTaskRunner{failPartition: 1}
	observer := &recordingTaskSetObserver{}
	taskSet := scheduler.TaskSet{
		JobID: 7, StageID: 3, StageAttemptID: 2,
		Tasks: []scheduler.Task{{ID: 20, StageID: 3, PartitionID: 0}, {ID: 21, StageID: 3, PartitionID: 1}},
	}

	if err := NewLocalTaskScheduler(runner).ScheduleTaskSet(context.Background(), taskSet, observer); err != nil {
		t.Fatalf("ScheduleTaskSet() error = %v", err)
	}
	successes, failures := observer.reports()
	if len(successes) != 1 || len(failures) != 1 {
		t.Fatalf("reports = %d successes, %d failures; want 1, 1", len(successes), len(failures))
	}
	failure := failures[0]
	if failure.JobID != 7 || failure.StageID != 3 || failure.PartitionID != 1 || failure.Error != "injected failure" {
		t.Fatalf("failure report = %#v, want job/stage/partition identity and injected failure", failure)
	}
}

func TestLocalTaskSchedulerRejectsMissingDependencies(t *testing.T) {
	taskSet := scheduler.TaskSet{Tasks: []scheduler.Task{{}}}
	observer := &recordingTaskSetObserver{}
	tests := []struct {
		name      string
		scheduler *LocalTaskScheduler
		observer  scheduler.TaskSetObserver
	}{
		{name: "runner", scheduler: NewLocalTaskScheduler(nil), observer: observer},
		{name: "observer", scheduler: NewLocalTaskScheduler(&immediateTaskRunner{})},
		{name: "tasks", scheduler: NewLocalTaskScheduler(&immediateTaskRunner{}), observer: observer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := taskSet
			if test.name == "tasks" {
				input.Tasks = nil
			}
			if err := test.scheduler.ScheduleTaskSet(context.Background(), input, test.observer); err == nil {
				t.Fatal("ScheduleTaskSet() error = nil, want validation error")
			}
		})
	}
}

type immediateTaskRunner struct {
	failPartition plan.PartitionID
}

func (r *immediateTaskRunner) RunTask(_ context.Context, task scheduler.Task) (scheduler.TaskOutput, error) {
	if task.PartitionID == r.failPartition && r.failPartition != 0 {
		return scheduler.TaskOutput{}, errors.New("injected failure")
	}
	return scheduler.TaskOutput{Count: int64(task.PartitionID) + 1}, nil
}

type recordingTaskSetObserver struct {
	mu        sync.Mutex
	successes []scheduler.TaskAttemptSuccess
	failures  []scheduler.TaskAttemptFailure
}

func (o *recordingTaskSetObserver) TaskSucceeded(report scheduler.TaskAttemptSuccess) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.successes = append(o.successes, report)
}

func (o *recordingTaskSetObserver) TaskFailed(report scheduler.TaskAttemptFailure) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, report)
}

func (o *recordingTaskSetObserver) reports() ([]scheduler.TaskAttemptSuccess, []scheduler.TaskAttemptFailure) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]scheduler.TaskAttemptSuccess(nil), o.successes...),
		append([]scheduler.TaskAttemptFailure(nil), o.failures...)
}
