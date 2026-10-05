package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// Worker errors let callers classify rejected operations without parsing text.
var (
	ErrUnknownWorker        = errors.New("unknown worker")
	ErrWorkerLost           = errors.New("worker is lost")
	ErrWorkerConflict       = errors.New("worker registration conflict")
	ErrInvalidWorker        = errors.New("invalid worker registration")
	ErrInvalidResourceOffer = errors.New("invalid resource offer")
)

type WorkerStatus string

const (
	WorkerAlive WorkerStatus = "alive"
	WorkerLost  WorkerStatus = "lost"
)

// WorkerSnapshot separates the last heartbeat from coordinator reservations.
// ReservedSlots includes assignments in transit and canceled work not yet reported.
type WorkerSnapshot struct {
	ID                plan.WorkerID
	Status            WorkerStatus
	TotalSlots        int
	ReportedFreeSlots int
	RunningAttemptIDs []plan.TaskAttemptID
	LastHeartbeat     time.Time
	ReservedSlots     int
}

type workerState struct {
	WorkerSnapshot
	reserved map[plan.TaskAttemptID]struct{}
}

// WorkerRegistry is owned by FIFOTaskScheduler and protected by its mutex.
// Lost identities remain registered so late reports can be checked and fenced.
type WorkerRegistry struct {
	workers map[plan.WorkerID]*workerState
}

func (r *WorkerRegistry) register(id plan.WorkerID, slots int, now time.Time) error {
	if id == "" || slots <= 0 {
		return fmt.Errorf("%w: worker ID must be nonempty and slots positive", ErrInvalidWorker)
	}
	if worker := r.workers[id]; worker != nil {
		if worker.Status == WorkerLost {
			return fmt.Errorf("%w %q: restart with a new worker ID", ErrWorkerLost, id)
		}
		if worker.TotalSlots != slots {
			return fmt.Errorf("%w: worker %q already has capacity %d", ErrWorkerConflict, id, worker.TotalSlots)
		}
		return nil
	}
	r.workers[id] = &workerState{WorkerSnapshot: WorkerSnapshot{ID: id, Status: WorkerAlive, TotalSlots: slots, LastHeartbeat: now}, reserved: make(map[plan.TaskAttemptID]struct{})}
	return nil
}

func (r *WorkerRegistry) snapshot(id plan.WorkerID) (WorkerSnapshot, error) {
	worker := r.workers[id]
	if worker == nil {
		return WorkerSnapshot{}, fmt.Errorf("%w %q", ErrUnknownWorker, id)
	}
	snapshot := worker.WorkerSnapshot
	snapshot.RunningAttemptIDs = append([]plan.TaskAttemptID(nil), worker.RunningAttemptIDs...)
	sort.Slice(snapshot.RunningAttemptIDs, func(i, j int) bool { return snapshot.RunningAttemptIDs[i] < snapshot.RunningAttemptIDs[j] })
	snapshot.ReservedSlots = len(worker.reserved)
	return snapshot, nil
}
