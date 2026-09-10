package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestSchedulerEventsCarryJobAndTaskIdentity(t *testing.T) {
	graph := plan.NewRDDGraph()
	response := make(chan jobCompletion, 1)
	submission := jobSubmitted{
		jobID:    3,
		ctx:      context.Background(),
		graph:    graph,
		action:   ActionSpec{Kind: ActionCount, TargetRDD: 7},
		response: response,
	}
	if submission.jobID != 3 || submission.graph != graph || submission.action.TargetRDD != 7 {
		t.Fatalf("job submission = %#v, want job 3 targeting RDD 7", submission)
	}

	success := taskSucceeded{TaskAttemptSuccess{JobID: 3, StageID: 2, Attempt: TaskAttemptIdentity{ID: 5, TaskID: 11, StageAttemptID: 1}, PartitionID: 4, Output: TaskOutput{Count: 9}}}
	if success.JobID != 3 || success.Attempt.TaskID != 11 || success.StageID != 2 || success.PartitionID != 4 {
		t.Fatalf("task success identity = %#v", success)
	}
	failure := taskFailed{TaskAttemptFailure{JobID: 3, StageID: 2, Attempt: success.Attempt, PartitionID: 4, Error: "task failed"}}
	if failure.Error != "task failed" || failure.Attempt != success.Attempt {
		t.Fatalf("task failure = %#v", failure)
	}

	cancellation := jobCanceled{jobID: 3, err: context.Canceled}
	if cancellation.jobID != 3 || !errors.Is(cancellation.err, context.Canceled) {
		t.Fatalf("job cancellation = %#v", cancellation)
	}
}

func TestAllEventTypesImplementSchedulerEvent(t *testing.T) {
	events := []schedulerEvent{
		jobSubmitted{},
		taskSucceeded{},
		taskFailed{},
		taskSetFinished{},
		jobCanceled{},
		schedulerStopping{},
	}
	if len(events) != 6 {
		t.Fatalf("event count = %d, want 6", len(events))
	}
}
