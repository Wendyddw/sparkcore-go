package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// TaskRunner executes one logical partition outside the scheduler event loop.
type TaskRunner interface {
	RunTask(context.Context, Task) (TaskOutput, error)
}

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
	action    ActionSpec
	stageID   plan.StageID
	status    stageExecutionStatus
	remaining int
	outputs   map[plan.PartitionID]TaskOutput
	response  chan<- jobCompletion
	cancel    context.CancelFunc
}

// DAGScheduler coordinates action jobs through one state-owning event loop.
type DAGScheduler struct {
	planner   *Planner
	runner    TaskRunner
	loop      *eventLoop
	jobs      map[plan.JobID]*jobState
	nextJobID atomic.Uint64
}

// NewDAGScheduler creates and starts an in-process DAG scheduler.
func NewDAGScheduler(functions FunctionLookup, runner TaskRunner) *DAGScheduler {
	scheduler := &DAGScheduler{
		planner: NewPlanner(functions),
		runner:  runner,
		jobs:    make(map[plan.JobID]*jobState),
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

// Close stops an idle scheduler. Active-job shutdown is completed separately.
func (s *DAGScheduler) Close() {
	if s != nil && s.loop != nil {
		s.loop.close()
	}
}

func (s *DAGScheduler) handleEvent(event schedulerEvent) {
	switch event := event.(type) {
	case jobSubmitted:
		s.handleJobSubmitted(event)
	case localTaskSucceeded:
		s.handleTaskSucceeded(event)
	case localTaskFailed:
		s.failJob(event.jobID, fmt.Errorf(
			"stage %d task %d partition %d: %w",
			event.stageID,
			event.taskID,
			event.partition,
			event.err,
		))
	case jobCanceled:
		s.failJob(event.jobID, event.err)
	}
}

func (s *DAGScheduler) handleJobSubmitted(event jobSubmitted) {
	if s.runner == nil {
		event.response <- jobCompletion{err: fmt.Errorf("task runner is nil")}
		return
	}
	stagePlan, err := s.planner.Plan(event.graph, event.action)
	if err != nil {
		event.response <- jobCompletion{err: err}
		return
	}
	if len(stagePlan.Stages) != 1 || stagePlan.Stages[0].Kind != StageResult || len(stagePlan.Stages[0].ParentIDs) != 0 {
		event.response <- jobCompletion{err: fmt.Errorf("shuffle execution is not implemented in Week 1")}
		return
	}
	tasks, err := GenerateTasks(stagePlan)
	if err != nil {
		event.response <- jobCompletion{err: err}
		return
	}

	jobCtx, cancel := context.WithCancel(event.ctx)
	s.jobs[event.jobID] = &jobState{
		action:    event.action,
		stageID:   stagePlan.Stages[0].ID,
		status:    stageRunning,
		remaining: len(tasks),
		outputs:   make(map[plan.PartitionID]TaskOutput, len(tasks)),
		response:  event.response,
		cancel:    cancel,
	}
	go s.watchCancellation(event.jobID, event.ctx, jobCtx)
	for _, task := range tasks {
		go s.executeTask(jobCtx, event.jobID, task)
	}
}

func (s *DAGScheduler) executeTask(ctx context.Context, jobID plan.JobID, task Task) {
	output, err := s.runner.RunTask(ctx, task)
	var event schedulerEvent = localTaskSucceeded{
		jobID:     jobID,
		taskID:    task.ID,
		stageID:   task.StageID,
		partition: task.PartitionID,
		output:    output,
	}
	if err != nil {
		event = localTaskFailed{
			jobID:     jobID,
			taskID:    task.ID,
			stageID:   task.StageID,
			partition: task.PartitionID,
			err:       err,
		}
	}
	_ = s.loop.send(context.Background(), event)
}

func (s *DAGScheduler) watchCancellation(jobID plan.JobID, callerCtx, jobCtx context.Context) {
	select {
	case <-callerCtx.Done():
		_ = s.loop.send(context.Background(), jobCanceled{jobID: jobID, err: callerCtx.Err()})
	case <-jobCtx.Done():
	}
}

func (s *DAGScheduler) handleTaskSucceeded(event localTaskSucceeded) {
	job, ok := s.jobs[event.jobID]
	if !ok || job.status != stageRunning || event.stageID != job.stageID {
		return
	}
	if _, duplicate := job.outputs[event.partition]; duplicate {
		return
	}
	job.outputs[event.partition] = event.output
	job.remaining--
	if job.remaining != 0 {
		return
	}

	job.status = stageSucceeded
	result := mergeTaskOutputs(job.action.Kind, job.outputs)
	job.cancel()
	job.response <- jobCompletion{result: result}
	delete(s.jobs, event.jobID)
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
