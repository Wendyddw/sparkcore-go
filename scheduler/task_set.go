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
// safe for concurrent calls from physical task schedulers. The scheduler must
// filter obsolete attempts before delivering reports.
type TaskSetObserver interface {
	TaskSucceeded(TaskAttemptSuccess)
	TaskFailed(TaskAttemptFailure)
}

// TaskSetScheduler schedules physical attempts for one stage's logical tasks.
// ScheduleTaskSet delivers individual attempt outcomes through observer and
// returns after all scheduling work and callbacks have exited. Implementations
// must honor ctx cancellation and support concurrent task-set submissions.
// A nil return requires every task to have a terminal report; a non-nil error
// terminates the task set even if some tasks have not reported.
type TaskSetScheduler interface {
	ScheduleTaskSet(context.Context, TaskSet, TaskSetObserver) error
}
