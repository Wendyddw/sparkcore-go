package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
)

// CoordinatorClient methods must support concurrent calls and context cancellation.
type CoordinatorClient interface {
	RegisterWorker(context.Context, protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error)
	Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error)
	ReportSuccess(context.Context, protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error)
	ReportFailure(context.Context, protocol.TaskFailureRequest) (protocol.TaskReportResponse, error)
}

var _ CoordinatorClient = (*Client)(nil)

// RuntimeConfig defaults to 100ms polling and 10s per coordinator call.
// RegisterFunctions initializes a fresh worker-owned registry before Run.
// Sources defaults to TextSourceReader; source reads begin only during execution.
type RuntimeConfig struct {
	WorkerID          plan.WorkerID
	Slots             int
	HeartbeatInterval time.Duration
	RequestTimeout    time.Duration
	RegisterFunctions func(*executor.FunctionRegistry) error
	Sources           executor.SourceReader
	Logger            *slog.Logger // Nil disables runtime lifecycle logs.
}

// Runtime owns one worker's execution lifecycle. Use a new instance per startup.
type Runtime struct {
	logger  *slog.Logger
	client  CoordinatorClient
	config  RuntimeConfig
	runner  *executor.LocalRunner
	started atomic.Bool
}

// NewRuntime creates a worker with its own function registry and LocalRunner.
func NewRuntime(client CoordinatorClient, config RuntimeConfig) (*Runtime, error) {
	if client == nil {
		return nil, fmt.Errorf("coordinator client is nil")
	}
	if err := (protocol.RegisterWorkerRequest{WorkerID: config.WorkerID, TotalSlots: config.Slots}).Validate(); err != nil {
		return nil, err
	}
	if config.HeartbeatInterval < 0 || config.RequestTimeout < 0 {
		return nil, fmt.Errorf("worker intervals and timeouts must not be negative")
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 100 * time.Millisecond
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	registry := executor.NewFunctionRegistry()
	if config.RegisterFunctions != nil {
		if err := config.RegisterFunctions(registry); err != nil {
			return nil, fmt.Errorf("register worker functions: %w", err)
		}
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Runtime{logger: logger.With("component", "worker", "worker_id", config.WorkerID),
		client: client, config: config, runner: executor.NewLocalRunner(registry, config.Sources, config.Slots)}, nil
}

// runtimeState belongs to one Run call. Only the event loop mutates its maps;
// task goroutines signal completion through the channel and wait group.
type runtimeState struct {
	active    map[plan.TaskAttemptID]protocol.TaskAssignment
	seen      map[plan.TaskAttemptID]bool
	completed chan plan.TaskAttemptID
	tasks     sync.WaitGroup
}

// Run registers once and polls until cancellation or a communication/protocol error.
// Cancellation stops execution, allows bounded terminal reports, and waits for tasks.
// Sources and functions must cooperate with cancellation for shutdown to finish.
func (w *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("worker context is nil")
	}
	if !w.started.CompareAndSwap(false, true) {
		return fmt.Errorf("worker runtime has already started")
	}
	ctx, stop := context.WithCancelCause(ctx)
	state := runtimeState{
		active:    make(map[plan.TaskAttemptID]protocol.TaskAssignment),
		seen:      make(map[plan.TaskAttemptID]bool),
		completed: make(chan plan.TaskAttemptID, w.config.Slots),
	}
	defer func() { stop(context.Canceled); state.tasks.Wait() }()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := w.register(ctx); err != nil {
		return err
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case id := <-state.completed:
			delete(state.active, id)
		case <-timer.C:
			state.drainCompleted()
			assignments, err := w.poll(ctx, &state)
			if err != nil {
				return err
			}
			w.dispatch(ctx, stop, &state, assignments)
			timer.Reset(w.config.HeartbeatInterval)
		}
	}
}

func (w *Runtime) register(ctx context.Context) error {
	request := protocol.RegisterWorkerRequest{WorkerID: w.config.WorkerID, TotalSlots: w.config.Slots}
	callCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	defer cancel()
	registration, err := w.client.RegisterWorker(callCtx, request)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	if err := registration.Validate(); err != nil || registration.WorkerID != request.WorkerID || registration.TotalSlots != request.TotalSlots {
		return fmt.Errorf("%w: registration does not match worker", ErrInvalidResponse)
	}
	w.logger.Info("worker_registered", "slots", w.config.Slots)
	return nil
}

// Drain acknowledged reports before taking the next capacity snapshot.
func (s *runtimeState) drainCompleted() {
	for {
		select {
		case id := <-s.completed:
			delete(s.active, id)
		default:
			return
		}
	}
}

func (w *Runtime) resourceOffer(active map[plan.TaskAttemptID]protocol.TaskAssignment) protocol.HeartbeatRequest {
	offer := protocol.HeartbeatRequest{
		WorkerID:          w.config.WorkerID,
		FreeSlots:         w.config.Slots - len(active),
		RunningAttemptIDs: make([]plan.TaskAttemptID, 0, len(active)),
	}
	for id := range active {
		offer.RunningAttemptIDs = append(offer.RunningAttemptIDs, id)
	}
	sort.Slice(offer.RunningAttemptIDs, func(i, j int) bool { return offer.RunningAttemptIDs[i] < offer.RunningAttemptIDs[j] })
	return offer
}

// poll returns a fully validated batch before any task can be dispatched.
func (w *Runtime) poll(ctx context.Context, state *runtimeState) ([]protocol.TaskAssignment, error) {
	offer := w.resourceOffer(state.active)
	callCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	defer cancel()
	response, err := w.client.Heartbeat(callCtx, offer)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("worker heartbeat: %w", err)
	}
	if err := validateAssignments(response, offer, state.active, state.seen); err != nil {
		return nil, err
	}
	return response.Assignments, nil
}

// dispatch reserves each attempt on the event loop before launching its goroutine.
func (w *Runtime) dispatch(ctx context.Context, stop context.CancelCauseFunc, state *runtimeState, assignments []protocol.TaskAssignment) {
	for _, assignment := range assignments {
		state.active[assignment.Attempt.ID] = assignment
		state.seen[assignment.Attempt.ID] = true
		state.tasks.Add(1)
		go func() {
			defer state.tasks.Done()
			if err := w.executeAndReport(ctx, assignment); err != nil {
				stop(fmt.Errorf("report attempt %d: %w", assignment.Attempt.ID, err))
			}
			select {
			case state.completed <- assignment.Attempt.ID:
			case <-ctx.Done():
			}
		}()
	}
}

func validateAssignments(response protocol.HeartbeatResponse, offer protocol.HeartbeatRequest, active map[plan.TaskAttemptID]protocol.TaskAssignment, seen map[plan.TaskAttemptID]bool) error {
	if err := response.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResponse, err)
	}
	if len(response.Assignments) > offer.FreeSlots {
		return fmt.Errorf("%w: assignments exceed local free slots", ErrInvalidResponse)
	}
	for _, a := range response.Assignments {
		if a.WorkerID != offer.WorkerID || seen[a.Attempt.ID] {
			return fmt.Errorf("%w: foreign or reused attempt %d", ErrInvalidResponse, a.Attempt.ID)
		}
		for _, running := range active {
			if running.JobID == a.JobID && running.StageID == a.StageID && running.Attempt.StageAttemptID == a.Attempt.StageAttemptID && running.Attempt.TaskID == a.Attempt.TaskID {
				return fmt.Errorf("%w: task %d is already active", ErrInvalidResponse, a.Attempt.TaskID)
			}
		}
	}
	return nil
}
