package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// ErrTaskSchedulerClosed indicates that no more placement or reports are accepted.
var ErrTaskSchedulerClosed = errors.New("FIFO task scheduler is closed")

// Report errors distinguish unknown attempts, identity conflicts and invalid values.
var (
	ErrUnknownAttempt    = errors.New("unknown attempt")
	ErrMismatchedReport  = errors.New("mismatched task report")
	ErrInvalidTaskReport = errors.New("invalid task report")
)

// TaskAssignment carries one physical assignment with its enclosing job identity.
type TaskAssignment struct {
	JobID   plan.JobID
	StageID plan.StageID
	Attempt TaskAttempt
}

type taskSetKey struct {
	job     plan.JobID
	stage   plan.StageID
	attempt plan.StageAttemptID
}
type logicalTaskState struct {
	task   Task
	state  TaskState
	active plan.TaskAttemptID
	output TaskOutput
}
type fifoTaskSet struct {
	key       taskSetKey
	sequence  uint64
	ctx       context.Context
	tasks     []*logicalTaskState
	pending   int
	remaining int
	reports   []terminalReport
	wake      chan struct{}
	terminal  bool
	err       error
}
type terminalReport struct {
	success *TaskAttemptSuccess
	failure *TaskAttemptFailure
}
type assignedAttempt struct {
	assignment TaskAssignment
	set        *fifoTaskSet
	task       *logicalTaskState
	terminal   bool
}

// FIFOTaskScheduler owns task placement and worker reservations under one mutex.
// Task-set and attempt history are retained for this scheduler instance's lifetime
// to reject reused submissions and recognize duplicate terminal reports.
// It never reads RDD lineage. Observer callbacks run in ScheduleTaskSet, outside
// the mutex; worker-facing methods only update state and enqueue reports.
type FIFOTaskScheduler struct {
	mu           sync.Mutex
	registry     WorkerRegistry
	sets         map[taskSetKey]*fifoTaskSet
	queue        []*fifoTaskSet
	attempts     map[plan.TaskAttemptID]*assignedAttempt
	nextSequence uint64
	nextAttempt  plan.TaskAttemptID
	closed       bool
	submissions  sync.WaitGroup
}

var _ TaskSetScheduler = (*FIFOTaskScheduler)(nil)

// NewFIFOTaskScheduler creates an empty scheduler with no workers or background loops.
func NewFIFOTaskScheduler() *FIFOTaskScheduler {
	return &FIFOTaskScheduler{
		registry: WorkerRegistry{workers: make(map[plan.WorkerID]*workerState)},
		sets:     make(map[taskSetKey]*fifoTaskSet),
		attempts: make(map[plan.TaskAttemptID]*assignedAttempt),
	}
}

// RegisterWorker is idempotent when the worker ID and configured capacity match.
func (s *FIFOTaskScheduler) RegisterWorker(id plan.WorkerID, slots int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrTaskSchedulerClosed
	}
	return s.registry.register(id, slots)
}

// Worker returns a detached snapshot of heartbeat data and current reservations.
func (s *FIFOTaskScheduler) Worker(id plan.WorkerID) (WorkerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registry.snapshot(id)
}

// ScheduleTaskSet queues one stage attempt, delivers accepted reports, and waits
// for completion or cancellation. Failed tasks are terminal; Week 2 never retries.
func (s *FIFOTaskScheduler) ScheduleTaskSet(ctx context.Context, input TaskSet, observer TaskSetObserver) error {
	if observer == nil {
		return fmt.Errorf("task-set observer is nil")
	}
	set, err := prepareTaskSet(ctx, input)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrTaskSchedulerClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if _, exists := s.sets[set.key]; exists {
		s.mu.Unlock()
		return fmt.Errorf("task set %+v already submitted", set.key)
	}
	set.sequence = s.nextSequence
	s.nextSequence++
	s.sets[set.key] = set
	s.queue = append(s.queue, set)
	s.submissions.Add(1)
	s.mu.Unlock()
	defer s.submissions.Done()
	for {
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.finish(set, err)
		}
		reports := set.reports
		set.reports = nil
		terminal, err := set.terminal, set.err
		s.mu.Unlock()
		for _, report := range reports {
			if report.success != nil {
				observer.TaskSucceeded(*report.success)
			} else {
				observer.TaskFailed(*report.failure)
			}
		}
		if terminal {
			return err
		}
		select {
		case <-set.wake:
		case <-ctx.Done():
		}
	}
}

func prepareTaskSet(ctx context.Context, input TaskSet) (*fifoTaskSet, error) {
	if len(input.Tasks) == 0 {
		return nil, fmt.Errorf("task set has no tasks")
	}
	set := &fifoTaskSet{key: taskSetKey{input.JobID, input.StageID, input.StageAttemptID}, ctx: ctx, remaining: len(input.Tasks), wake: make(chan struct{}, 1)}
	ids := make(map[plan.TaskID]bool)
	partitions := make(map[plan.PartitionID]bool)
	for _, task := range input.Tasks {
		if task.StageID != input.StageID || task.PartitionID < 0 || ids[task.ID] || partitions[task.PartitionID] {
			return nil, fmt.Errorf("invalid or duplicate task %d partition %d in stage %d", task.ID, task.PartitionID, input.StageID)
		}
		ids[task.ID] = true
		partitions[task.PartitionID] = true
		set.tasks = append(set.tasks, &logicalTaskState{task: cloneTask(task), state: TaskPending})
	}
	sort.Slice(set.tasks, func(i, j int) bool { return set.tasks[i].task.PartitionID < set.tasks[j].task.PartitionID })
	return set, nil
}
func cloneTask(task Task) Task {
	task.Operations = cloneStageOperations(task.Operations)
	task.ShuffleWrite = cloneShuffleWrite(task.ShuffleWrite)
	task.FinalAction = cloneAction(task.FinalAction)
	return task
}

// OfferResources records a heartbeat and assigns the oldest pending partitions.
// Running IDs may include already-reported attempts because reports and heartbeats
// can cross in transit. Reservations absent from this heartbeat consume free slots.
func (s *FIFOTaskScheduler) OfferResources(id plan.WorkerID, free int, running []plan.TaskAttemptID) ([]TaskAssignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrTaskSchedulerClosed
	}
	worker := s.registry.workers[id]
	if worker == nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownWorker, id)
	}
	if free < 0 || free > worker.TotalSlots || len(running) > worker.TotalSlots-free {
		return nil, fmt.Errorf("%w: invalid capacity for worker %q", ErrInvalidResourceOffer, id)
	}
	seen := make(map[plan.TaskAttemptID]bool, len(running))
	for _, attemptID := range running {
		attempt := s.attempts[attemptID]
		if seen[attemptID] || attempt == nil || attempt.assignment.Attempt.WorkerID != id {
			return nil, fmt.Errorf("%w: invalid running attempt %d for worker %q", ErrInvalidResourceOffer, attemptID, id)
		}
		seen[attemptID] = true
	}
	worker.ReportedFreeSlots = free
	worker.RunningAttemptIDs = append([]plan.TaskAttemptID(nil), running...)
	worker.LastHeartbeat = time.Now()
	available := free
	for attemptID := range worker.reserved {
		if !seen[attemptID] {
			available--
		}
	}
	available = min(available, worker.TotalSlots-len(worker.reserved))
	var assignments []TaskAssignment
	for _, set := range s.queue {
		if set.terminal {
			continue
		}
		if err := set.ctx.Err(); err != nil {
			s.finish(set, err)
			continue
		}
		for available > 0 && set.pending < len(set.tasks) {
			task := set.tasks[set.pending]
			set.pending++
			identity := TaskAttemptIdentity{ID: s.nextAttempt, TaskID: task.task.ID, StageAttemptID: set.key.attempt}
			s.nextAttempt++
			task.state = TaskRunning
			task.active = identity.ID
			assignment := TaskAssignment{JobID: set.key.job, StageID: set.key.stage, Attempt: TaskAttempt{Identity: identity, Task: cloneTask(task.task), WorkerID: id, State: TaskRunning}}
			s.attempts[identity.ID] = &assignedAttempt{assignment: assignment, set: set, task: task}
			worker.reserved[identity.ID] = struct{}{}
			// The returned pipeline must not alias scheduler-owned metadata.
			assignment.Attempt.Task = cloneTask(assignment.Attempt.Task)
			assignments = append(assignments, assignment)
			available--
		}
	}
	s.compactQueue()
	return assignments, nil
}

// ReportSuccess accepts an attempt once and queues its partition output.
// Record payloads are treated as immutable after reporting.
func (s *FIFOTaskScheduler) ReportSuccess(report TaskAttemptSuccess) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempt, err := s.validateReport(report.JobID, report.StageID, report.Attempt, report.PartitionID, report.WorkerID)
	if err != nil || attempt == nil {
		return err
	}
	s.release(attempt, TaskSucceeded)
	if attempt.set.terminal {
		return nil
	}
	if err := attempt.set.ctx.Err(); err != nil {
		s.finish(attempt.set, err)
		return nil
	}
	report.Output.Records = append([]any(nil), report.Output.Records...)
	attempt.task.output = report.Output
	attempt.task.output.Records = append([]any(nil), report.Output.Records...)
	attempt.set.remaining--
	attempt.set.reports = append(attempt.set.reports, terminalReport{success: &report})
	if attempt.set.remaining == 0 {
		s.finish(attempt.set, nil)
	} else {
		s.notify(attempt.set)
	}
	return nil
}

// ReportFailure releases this attempt and terminates its task set without retry.
func (s *FIFOTaskScheduler) ReportFailure(report TaskAttemptFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if report.Error == "" {
		return fmt.Errorf("%w: task failure message is empty", ErrInvalidTaskReport)
	}
	attempt, err := s.validateReport(report.JobID, report.StageID, report.Attempt, report.PartitionID, report.WorkerID)
	if err != nil || attempt == nil {
		return err
	}
	s.release(attempt, TaskFailed)
	if attempt.set.terminal {
		return nil
	}
	if err := attempt.set.ctx.Err(); err != nil {
		s.finish(attempt.set, err)
		return nil
	}
	attempt.set.reports = append(attempt.set.reports, terminalReport{failure: &report})
	s.finish(attempt.set, fmt.Errorf("stage %d task %d partition %d: %s", report.StageID, report.Attempt.TaskID, report.PartitionID, report.Error))
	return nil
}

// Unknown or mismatched identities are errors; known terminal/obsolete reports
// are no-ops. Retaining attempt history distinguishes duplicates from unknown IDs.
func (s *FIFOTaskScheduler) validateReport(job plan.JobID, stage plan.StageID, identity TaskAttemptIdentity, partition plan.PartitionID, worker plan.WorkerID) (*assignedAttempt, error) {
	if s.closed {
		return nil, ErrTaskSchedulerClosed
	}
	if s.registry.workers[worker] == nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownWorker, worker)
	}
	attempt := s.attempts[identity.ID]
	if attempt == nil {
		return nil, fmt.Errorf("%w %d", ErrUnknownAttempt, identity.ID)
	}
	a := attempt.assignment
	if a.JobID != job || a.StageID != stage || a.Attempt.Identity != identity || a.Attempt.Task.PartitionID != partition || a.Attempt.WorkerID != worker {
		return nil, fmt.Errorf("%w for attempt %d", ErrMismatchedReport, identity.ID)
	}
	if attempt.terminal || attempt.task.active != identity.ID {
		return nil, nil
	}
	return attempt, nil
}
func (s *FIFOTaskScheduler) release(attempt *assignedAttempt, state TaskState) {
	attempt.terminal = true
	attempt.assignment.Attempt.State = state
	attempt.task.state = state
	delete(s.registry.workers[attempt.assignment.Attempt.WorkerID].reserved, attempt.assignment.Attempt.Identity.ID)
}
func (s *FIFOTaskScheduler) notify(set *fifoTaskSet) {
	select {
	case set.wake <- struct{}{}:
	default:
	}
}
func (s *FIFOTaskScheduler) finish(set *fifoTaskSet, err error) {
	if set.terminal {
		return
	}
	set.terminal = true
	set.err = err
	// Remote siblings still occupy slots until their terminal reports arrive.
	s.notify(set)
}
func (s *FIFOTaskScheduler) compactQueue() {
	live := s.queue[:0]
	for _, set := range s.queue {
		if !set.terminal && set.pending < len(set.tasks) {
			live = append(live, set)
		}
	}
	clear(s.queue[len(live):])
	s.queue = live
}

// Close unblocks submissions and waits for their observer callbacks to finish.
// As with ScheduleTaskSet, observers must return; they must not call Close inline.
func (s *FIFOTaskScheduler) Close() {
	s.mu.Lock()
	s.closed = true
	for _, set := range s.sets {
		s.finish(set, ErrTaskSchedulerClosed)
	}
	s.mu.Unlock()
	s.submissions.Wait()
}
