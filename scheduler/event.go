package scheduler

import (
	"context"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// schedulerEvent is handled only by the scheduler event loop.
type schedulerEvent interface {
	isSchedulerEvent()
}

// jobSubmitted requests planning and execution of one action-triggered job.
type jobSubmitted struct {
	jobID    plan.JobID
	ctx      context.Context
	graph    *plan.RDDGraph
	action   ActionSpec
	response chan<- jobCompletion
}

func (jobSubmitted) isSchedulerEvent() {}

// taskSucceeded carries an accepted partition result from a physical scheduler.
type taskSucceeded struct{ TaskAttemptSuccess }

func (taskSucceeded) isSchedulerEvent() {}

// taskFailed carries an accepted terminal failure from a physical scheduler.
type taskFailed struct{ TaskAttemptFailure }

func (taskFailed) isSchedulerEvent() {}

// taskSetFinished ensures scheduling errors or incomplete submissions unblock the job.
type taskSetFinished struct {
	jobID          plan.JobID
	stageAttemptID plan.StageAttemptID
	err            error
}

func (taskSetFinished) isSchedulerEvent() {}

// jobCanceled requests cancellation of an active job.
type jobCanceled struct {
	jobID plan.JobID
	err   error
}

func (jobCanceled) isSchedulerEvent() {}

// schedulerStopping cancels active jobs before the event loop exits.
type schedulerStopping struct {
	done chan<- struct{}
}

func (schedulerStopping) isSchedulerEvent() {}

// TaskOutput remains partition-local until every task in its stage succeeds.
// Records use any to keep scheduler independent of executor implementation types.
type TaskOutput struct {
	Records []any
	Count   int64
}

// jobCompletion is delivered exactly once to the action caller.
type jobCompletion struct {
	result JobResult
	err    error
}
