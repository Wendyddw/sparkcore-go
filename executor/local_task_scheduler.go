package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

const localWorkerID plan.WorkerID = "local"

// LocalTaskScheduler runs a task set through an in-process TaskRunner.
type LocalTaskScheduler struct {
	runner        scheduler.TaskRunner
	nextAttemptID atomic.Uint64
}

var _ scheduler.TaskSetScheduler = (*LocalTaskScheduler)(nil)

// NewLocalTaskScheduler creates the physical scheduling adapter used by local mode.
func NewLocalTaskScheduler(runner scheduler.TaskRunner) *LocalTaskScheduler {
	return &LocalTaskScheduler{runner: runner}
}

// ScheduleTaskSet assigns one local attempt to every logical task and waits
// until all runner calls have exited.
func (s *LocalTaskScheduler) ScheduleTaskSet(
	ctx context.Context,
	taskSet scheduler.TaskSet,
	observer scheduler.TaskSetObserver,
) error {
	if s == nil || s.runner == nil {
		return fmt.Errorf("local task scheduler runner is nil")
	}
	if observer == nil {
		return fmt.Errorf("local task scheduler observer is nil")
	}
	if len(taskSet.Tasks) == 0 {
		return fmt.Errorf("local task set has no tasks")
	}

	var attempts sync.WaitGroup
	attempts.Add(len(taskSet.Tasks))
	for _, task := range taskSet.Tasks {
		identity := scheduler.TaskAttemptIdentity{
			ID:             plan.TaskAttemptID(s.nextAttemptID.Add(1) - 1),
			TaskID:         task.ID,
			StageAttemptID: taskSet.StageAttemptID,
		}
		go func() {
			defer attempts.Done()
			output, err := s.runner.RunTask(ctx, task)
			if err != nil {
				observer.TaskFailed(scheduler.TaskAttemptFailure{
					JobID:       taskSet.JobID,
					StageID:     taskSet.StageID,
					Attempt:     identity,
					PartitionID: task.PartitionID,
					WorkerID:    localWorkerID,
					Error:       err.Error(),
				})
				return
			}
			observer.TaskSucceeded(scheduler.TaskAttemptSuccess{
				JobID:       taskSet.JobID,
				StageID:     taskSet.StageID,
				Attempt:     identity,
				PartitionID: task.PartitionID,
				WorkerID:    localWorkerID,
				Output:      output,
			})
		}()
	}
	attempts.Wait()
	return nil
}
