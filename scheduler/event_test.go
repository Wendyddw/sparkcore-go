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

	success := localTaskSucceeded{
		jobID:     3,
		taskID:    11,
		stageID:   2,
		partition: 4,
		output:    taskOutput{count: 9},
	}
	if success.jobID != 3 || success.taskID != 11 || success.stageID != 2 || success.partition != 4 {
		t.Fatalf("task success identity = %#v", success)
	}

	sentinel := errors.New("task failed")
	failure := localTaskFailed{
		jobID:     3,
		taskID:    11,
		stageID:   2,
		partition: 4,
		err:       sentinel,
	}
	if !errors.Is(failure.err, sentinel) {
		t.Fatalf("task failure error = %v, want sentinel", failure.err)
	}

	cancellation := jobCanceled{jobID: 3, err: context.Canceled}
	if cancellation.jobID != 3 || !errors.Is(cancellation.err, context.Canceled) {
		t.Fatalf("job cancellation = %#v", cancellation)
	}
}

func TestAllEventTypesImplementSchedulerEvent(t *testing.T) {
	events := []schedulerEvent{
		jobSubmitted{},
		localTaskSucceeded{},
		localTaskFailed{},
		jobCanceled{},
	}
	if len(events) != 4 {
		t.Fatalf("event count = %d, want 4", len(events))
	}
}
