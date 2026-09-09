package scheduler

import (
	"context"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// TaskSet contains the logical partition tasks for one stage attempt.
type TaskSet struct {
	JobID          plan.JobID          `json:"job_id"`
	StageID        plan.StageID        `json:"stage_id"`
	StageAttemptID plan.StageAttemptID `json:"stage_attempt_id"`
	Tasks          []Task              `json:"tasks"`
}

// TaskSetObserver receives terminal attempt reports. Implementations must be
// safe for concurrent calls from physical task schedulers.
type TaskSetObserver interface {
	TaskSucceeded(TaskAttemptSuccess)
	TaskFailed(TaskAttemptFailure)
}

// TaskSetScheduler schedules physical attempts for one stage's logical tasks.
// ScheduleTaskSet blocks until its scheduling work exits or ctx is canceled;
// individual attempt outcomes are delivered through observer.
type TaskSetScheduler interface {
	ScheduleTaskSet(context.Context, TaskSet, TaskSetObserver) error
}
