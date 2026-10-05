package scheduler

import (
	"fmt"
	"sort"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// ExpireWorkers fences workers silent for at least timeout and retires their
// outstanding assignments through the task retry policy. Accepted outputs survive.
// A timeout is suspicion: the old process may still execute, but cannot publish
// accepted results or acquire more work under its lost worker ID.
func (s *FIFOTaskScheduler) ExpireWorkers(now time.Time, timeout time.Duration) ([]plan.WorkerID, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("worker timeout must be positive")
	}
	s.mu.Lock()
	var lost []plan.WorkerID
	defer func() {
		s.mu.Unlock()
		for _, id := range lost {
			s.logger.Warn("worker_lost", "worker_id", id)
		}
	}()
	if s.closed {
		return nil, ErrTaskSchedulerClosed
	}
	for id, worker := range s.registry.workers {
		if worker.Status == WorkerAlive && now.Sub(worker.LastHeartbeat) >= timeout {
			lost = append(lost, id)
		}
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i] < lost[j] })
	for _, id := range lost {
		worker := s.registry.workers[id]
		worker.Status = WorkerLost
		attempts := make([]plan.TaskAttemptID, 0, len(worker.reserved))
		for attempt := range worker.reserved {
			attempts = append(attempts, attempt)
		}
		sort.Slice(attempts, func(i, j int) bool { return attempts[i] < attempts[j] })
		for _, attemptID := range attempts {
			attempt := s.attempts[attemptID]
			a := attempt.assignment
			s.failAttempt(attempt, TaskAttemptFailure{Kind: FailureWorkerLost,
				JobID: a.JobID, StageID: a.StageID, Attempt: a.Attempt.Identity,
				PartitionID: a.Attempt.Task.PartitionID, WorkerID: id,
				Error: fmt.Sprintf("worker %q heartbeat expired", id)})
		}
	}
	s.compactQueue()
	return lost, nil
}
