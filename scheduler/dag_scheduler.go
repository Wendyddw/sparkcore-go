package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// DAGScheduler coordinates action jobs through one state-owning event loop.
type DAGScheduler struct {
	maxStageAttempts   int
	logger             *slog.Logger
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
func NewDAGScheduler(functions FunctionLookup, taskScheduler TaskSetScheduler, options ...Option) *DAGScheduler {
	config := resolveSchedulerOptions(options)
	scheduler := &DAGScheduler{
		maxStageAttempts: config.maxStageAttempts,
		logger:           config.loggerFor("dag_scheduler"),
		planner:          NewPlanner(functions),
		taskScheduler:    taskScheduler,
		jobs:             make(map[plan.JobID]*jobState),
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
		s.handleTaskFailed(event.TaskAttemptFailure)
	case taskSetFinished:
		job := s.jobs[event.jobID]
		if job == nil {
			return
		}
		stage := job.stages[event.stageID]
		if stage == nil || stage.status != stageRunning || stage.attemptID != event.stageAttemptID {
			return
		}
		err := event.err
		if job.ctx.Err() != nil {
			err = job.ctx.Err()
		}
		if err == nil {
			err = fmt.Errorf("task scheduler exited with %d unfinished partitions", stage.remaining)
		}
		s.failJob(event.jobID, err)
	case jobCanceled:
		s.failJob(event.jobID, event.err)
	case schedulerStopping:
		s.handleStopping(event)
	}
}

func (s *DAGScheduler) handleJobSubmitted(event jobSubmitted) {
	s.logger.Info("job_submitted", "job_id", event.jobID, "action", event.action.Kind, "rdd_id", event.action.TargetRDD)
	if s.stopping {
		s.rejectJob(event, ErrSchedulerClosed)
		return
	}
	if s.taskScheduler == nil {
		s.rejectJob(event, fmt.Errorf("task scheduler is nil"))
		return
	}
	stagePlan, err := s.planner.Plan(event.graph, event.action)
	if err != nil {
		s.rejectJob(event, err)
		return
	}
	if err := validateExecutionPlan(stagePlan); err != nil {
		s.rejectJob(event, err)
		return
	}
	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		s.rejectJob(event, err)
		return
	}
	jobCtx, cancel := context.WithCancel(event.ctx)
	job := &jobState{action: event.action, ctx: jobCtx, cancel: cancel, response: event.response,
		stages: make(map[plan.StageID]*stageState)}
	for _, stage := range stagePlan.Stages {
		job.order = append(job.order, stage.ID)
		job.stages[stage.ID] = &stageState{stage: stage, status: stagePending, outputs: make(map[plan.PartitionID]TaskOutput), remaining: stage.NumPartitions}
		if stage.Kind == StageResult {
			job.resultStageID = stage.ID
		}
	}
	for _, task := range tasks {
		job.stages[task.StageID].tasks = append(job.stages[task.StageID].tasks, task)
	}
	s.jobs[event.jobID] = job
	s.workers.Add(1)
	go s.watchCancellation(event.jobID, event.ctx, jobCtx)
	if err := s.startReadyStages(event.jobID, job); err != nil {
		s.failJob(event.jobID, err)
	}
}

func (s *DAGScheduler) rejectJob(event jobSubmitted, err error) {
	s.logger.Error("job_failed", "job_id", event.jobID, "action", event.action.Kind, "error", err)
	event.response <- jobCompletion{err: err}
}

func (s *DAGScheduler) scheduleTaskSet(ctx context.Context, taskSet TaskSet) {
	defer s.workers.Done()
	observer := dagTaskSetObserver{loop: s.loop, jobID: taskSet.JobID, stageID: taskSet.StageID, stageAttemptID: taskSet.StageAttemptID}
	err := s.taskScheduler.ScheduleTaskSet(ctx, taskSet, observer)
	_ = s.loop.send(context.Background(), taskSetFinished{jobID: taskSet.JobID, stageID: taskSet.StageID, stageAttemptID: taskSet.StageAttemptID, err: err})
}

// Callbacks only enqueue events; job state remains owned by the event loop.
// Scope each observer to its submission so a malformed report cannot affect another job.
type dagTaskSetObserver struct {
	loop           *eventLoop
	jobID          plan.JobID
	stageID        plan.StageID
	stageAttemptID plan.StageAttemptID
}

func (o dagTaskSetObserver) TaskSucceeded(report TaskAttemptSuccess) {
	if report.JobID == o.jobID && report.StageID == o.stageID && report.Attempt.StageAttemptID == o.stageAttemptID {
		report.Output = report.Output.Clone()
		_ = o.loop.send(context.Background(), taskSucceeded{report})
	}
}

func (o dagTaskSetObserver) TaskFailed(report TaskAttemptFailure) {
	if report.JobID == o.jobID && report.StageID == o.stageID && report.Attempt.StageAttemptID == o.stageAttemptID {
		if report.ShuffleInput != nil {
			copy := *report.ShuffleInput
			report.ShuffleInput = &copy
		}
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
	if job == nil {
		return nil
	}
	stage := job.stages[stageID]
	if stage == nil || stage.status != stageRunning || attempt.StageAttemptID != stage.attemptID || partition < 0 || int(partition) >= len(stage.tasks) {
		return nil
	}
	if stage.tasks[partition].ID != attempt.TaskID {
		return nil
	}
	if _, duplicate := stage.outputs[partition]; duplicate {
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
	stage := job.stages[report.StageID]
	runID := job.runID
	if runID == "" && report.Output.ShuffleOutput != nil {
		runID = report.Output.ShuffleOutput.Attempt.RunID
	}
	if err := report.Output.ValidateFor(TaskExecution{RunID: runID, JobID: report.JobID, Task: stage.tasks[report.PartitionID], Attempt: report.Attempt}); err != nil {
		s.failJob(report.JobID, fmt.Errorf("invalid stage output: %w", err))
		return
	}
	if stage.stage.Kind == StageShuffleMap {
		job.runID = runID
		s.logger.Info("shuffle_output_accepted", append(attemptLogFields(report.JobID, report.StageID, report.Attempt, report.PartitionID, report.WorkerID),
			"run_id", runID, "shuffle_id", report.Output.ShuffleOutput.Attempt.ShuffleID)...)
	}
	stage.outputs[report.PartitionID] = report.Output.Clone()
	stage.remaining--
	if stage.remaining != 0 {
		return
	}
	stage.status = stageSucceeded
	s.logger.Info("stage_succeeded", "job_id", report.JobID, "stage_id", report.StageID,
		"stage_attempt_id", stage.attemptID, "partition_count", len(stage.tasks))
	if report.StageID != job.resultStageID {
		if err := s.startReadyStages(report.JobID, job); err != nil {
			s.failJob(report.JobID, err)
		}
		return
	}
	result := mergeTaskOutputs(job.action.Kind, stage.outputs)
	s.logger.Info("job_succeeded", "job_id", report.JobID, "action", job.action.Kind,
		"count", result.Count, "record_count", len(result.Records))
	job.cancel()
	job.response <- jobCompletion{result: result}
	delete(s.jobs, report.JobID)
}

func (s *DAGScheduler) failJob(jobID plan.JobID, err error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return
	}
	s.logger.Error("job_failed", "job_id", jobID, "action", job.action.Kind, "error", err)
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
