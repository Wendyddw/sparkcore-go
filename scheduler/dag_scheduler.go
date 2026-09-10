package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// ErrSchedulerClosed indicates that the scheduler is shutting down or stopped.
var ErrSchedulerClosed = errors.New("DAG scheduler is closed")

// JobResult is the completed output of one action-triggered job.
type JobResult struct {
	Records []any
	Count   int64
}

type stageExecutionStatus uint8

const (
	stageRunning stageExecutionStatus = iota
	stageSucceeded
)

type jobState struct {
	action         ActionSpec
	stageID        plan.StageID
	stageAttemptID plan.StageAttemptID
	tasks          map[plan.PartitionID]plan.TaskID
	ctx            context.Context
	status         stageExecutionStatus
	remaining      int
	outputs        map[plan.PartitionID]TaskOutput
	response       chan<- jobCompletion
	cancel         context.CancelFunc
}

// DAGScheduler coordinates action jobs through one state-owning event loop.
type DAGScheduler struct {
	planner            *Planner
	taskScheduler      TaskSetScheduler
	loop               *eventLoop
	jobs               map[plan.JobID]*jobState
	nextJobID          atomic.Uint64
	nextStageAttemptID plan.StageAttemptID
	closing            atomic.Bool
	closeOnce          sync.Once
	workers            sync.WaitGroup
	stopping           bool
}

// NewDAGScheduler creates and starts an in-process DAG scheduler.
func NewDAGScheduler(functions FunctionLookup, taskScheduler TaskSetScheduler) *DAGScheduler {
	scheduler := &DAGScheduler{
		planner:       NewPlanner(functions),
		taskScheduler: taskScheduler,
		jobs:          make(map[plan.JobID]*jobState),
	}
	scheduler.loop = newEventLoop(scheduler.handleEvent)
	return scheduler
}

// Run submits an action and waits for its result.
func (s *DAGScheduler) Run(
	ctx context.Context,
	graph *plan.RDDGraph,
	action ActionSpec,
) (JobResult, error) {
	if s == nil || s.loop == nil {
		return JobResult{}, fmt.Errorf("run action %q for RDD %d: scheduler is not running", action.Kind, action.TargetRDD)
	}
	if s.closing.Load() {
		return JobResult{}, ErrSchedulerClosed
	}
	jobID := plan.JobID(s.nextJobID.Add(1) - 1)
	response := make(chan jobCompletion, 1)
	if err := s.loop.send(ctx, jobSubmitted{
		jobID:    jobID,
		ctx:      ctx,
		graph:    graph,
		action:   action,
		response: response,
	}); err != nil {
		return JobResult{}, fmt.Errorf("submit job %d: %w", jobID, err)
	}

	completion := <-response
	if completion.err != nil {
		return JobResult{}, fmt.Errorf("job %d: %w", jobID, completion.err)
	}
	return completion.result, nil
}

// Close cancels active jobs, waits for scheduler-owned goroutines, and stops the loop.
func (s *DAGScheduler) Close() {
	if s == nil || s.loop == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		stopped := make(chan struct{})
		if err := s.loop.send(context.Background(), schedulerStopping{done: stopped}); err == nil {
			<-stopped
		}
		s.workers.Wait()
		s.loop.close()
	})
}

func (s *DAGScheduler) handleEvent(event schedulerEvent) {
	switch event := event.(type) {
	case jobSubmitted:
		s.handleJobSubmitted(event)
	case taskSucceeded:
		s.handleTaskSucceeded(event.TaskAttemptSuccess)
	case taskFailed:
		report := event.TaskAttemptFailure
		if job := s.pendingTask(report.JobID, report.StageID, report.Attempt, report.PartitionID); job != nil {
			err := fmt.Errorf("stage %d task %d partition %d: %s", report.StageID, report.Attempt.TaskID, report.PartitionID, report.Error)
			if job.ctx.Err() != nil {
				err = job.ctx.Err()
			}
			s.failJob(report.JobID, err)
		}
	case taskSetFinished:
		if job := s.jobs[event.jobID]; job != nil && job.stageAttemptID == event.stageAttemptID {
			err := event.err
			if job.ctx.Err() != nil {
				err = job.ctx.Err()
			}
			if err == nil {
				err = fmt.Errorf("task scheduler exited with %d unfinished partitions", job.remaining)
			}
			s.failJob(event.jobID, err)
		}
	case jobCanceled:
		s.failJob(event.jobID, event.err)
	case schedulerStopping:
		s.handleStopping(event)
	}
}

func (s *DAGScheduler) handleJobSubmitted(event jobSubmitted) {
	if s.stopping {
		event.response <- jobCompletion{err: ErrSchedulerClosed}
		return
	}
	if s.taskScheduler == nil {
		event.response <- jobCompletion{err: fmt.Errorf("task scheduler is nil")}
		return
	}
	stagePlan, err := s.planner.Plan(event.graph, event.action)
	if err != nil {
		event.response <- jobCompletion{err: err}
		return
	}
	if len(stagePlan.Stages) != 1 || stagePlan.Stages[0].Kind != StageResult || len(stagePlan.Stages[0].ParentIDs) != 0 {
		event.response <- jobCompletion{err: fmt.Errorf("shuffle execution is not implemented")}
		return
	}
	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		event.response <- jobCompletion{err: err}
		return
	}

	jobCtx, cancel := context.WithCancel(event.ctx)
	taskSet := TaskSet{JobID: event.jobID, StageID: stagePlan.Stages[0].ID, StageAttemptID: s.nextStageAttemptID, Tasks: tasks}
	s.nextStageAttemptID++
	logicalTasks := make(map[plan.PartitionID]plan.TaskID, len(tasks))
	for _, task := range tasks {
		logicalTasks[task.PartitionID] = task.ID
	}
	s.jobs[event.jobID] = &jobState{
		action:         event.action,
		stageID:        taskSet.StageID,
		stageAttemptID: taskSet.StageAttemptID,
		tasks:          logicalTasks,
		ctx:            jobCtx,
		status:         stageRunning,
		remaining:      len(tasks),
		outputs:        make(map[plan.PartitionID]TaskOutput, len(tasks)),
		response:       event.response,
		cancel:         cancel,
	}
	s.workers.Add(1)
	go s.watchCancellation(event.jobID, event.ctx, jobCtx)
	s.workers.Add(1)
	go s.scheduleTaskSet(jobCtx, taskSet)
}

func (s *DAGScheduler) scheduleTaskSet(ctx context.Context, taskSet TaskSet) {
	defer s.workers.Done()
	observer := dagTaskSetObserver{loop: s.loop, jobID: taskSet.JobID}
	err := s.taskScheduler.ScheduleTaskSet(ctx, taskSet, observer)
	_ = s.loop.send(context.Background(), taskSetFinished{jobID: taskSet.JobID, stageAttemptID: taskSet.StageAttemptID, err: err})
}

// Callbacks only enqueue events; job state remains owned by the event loop.
// Scope each observer to its submission so a malformed report cannot affect another job.
type dagTaskSetObserver struct {
	loop  *eventLoop
	jobID plan.JobID
}

func (o dagTaskSetObserver) TaskSucceeded(report TaskAttemptSuccess) {
	if report.JobID == o.jobID {
		_ = o.loop.send(context.Background(), taskSucceeded{report})
	}
}

func (o dagTaskSetObserver) TaskFailed(report TaskAttemptFailure) {
	if report.JobID == o.jobID {
		_ = o.loop.send(context.Background(), taskFailed{report})
	}
}

func (s *DAGScheduler) watchCancellation(jobID plan.JobID, callerCtx, jobCtx context.Context) {
	defer s.workers.Done()
	select {
	case <-callerCtx.Done():
		_ = s.loop.send(context.Background(), jobCanceled{jobID: jobID, err: callerCtx.Err()})
	case <-jobCtx.Done():
	}
}

func (s *DAGScheduler) handleStopping(event schedulerStopping) {
	s.stopping = true
	for jobID := range s.jobs {
		s.failJob(jobID, ErrSchedulerClosed)
	}
	event.done <- struct{}{}
}

// pendingTask validates logical identity. The physical scheduler is responsible
// for accepting only the current task attempt before notifying the observer.
func (s *DAGScheduler) pendingTask(jobID plan.JobID, stageID plan.StageID, attempt TaskAttemptIdentity, partition plan.PartitionID) *jobState {
	job := s.jobs[jobID]
	if job == nil || job.status != stageRunning || stageID != job.stageID || attempt.StageAttemptID != job.stageAttemptID {
		return nil
	}
	taskID, exists := job.tasks[partition]
	if !exists || taskID != attempt.TaskID {
		return nil
	}
	if _, duplicate := job.outputs[partition]; duplicate {
		return nil
	}
	return job
}

func (s *DAGScheduler) handleTaskSucceeded(report TaskAttemptSuccess) {
	job := s.pendingTask(report.JobID, report.StageID, report.Attempt, report.PartitionID)
	if job == nil {
		return
	}
	if err := job.ctx.Err(); err != nil {
		s.failJob(report.JobID, err)
		return
	}
	job.outputs[report.PartitionID] = report.Output
	job.remaining--
	if job.remaining != 0 {
		return
	}

	job.status = stageSucceeded
	result := mergeTaskOutputs(job.action.Kind, job.outputs)
	job.cancel()
	job.response <- jobCompletion{result: result}
	delete(s.jobs, report.JobID)
}

func (s *DAGScheduler) failJob(jobID plan.JobID, err error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return
	}
	job.cancel()
	job.response <- jobCompletion{err: err}
	delete(s.jobs, jobID)
}

func mergeTaskOutputs(kind ActionKind, outputs map[plan.PartitionID]TaskOutput) JobResult {
	var result JobResult
	for partition := plan.PartitionID(0); partition < plan.PartitionID(len(outputs)); partition++ {
		output := outputs[partition]
		switch kind {
		case ActionCollect:
			result.Records = append(result.Records, output.Records...)
		case ActionCount:
			result.Count += output.Count
		}
	}
	return result
}
