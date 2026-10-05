package scheduler

import (
	"log/slog"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// Option configures a scheduler at construction time.
type Option func(*schedulerOptions)

type schedulerOptions struct {
	logger          *slog.Logger
	maxTaskAttempts int
	now             func() time.Time
}

// WithClock supplies FIFO's registration and heartbeat clock. It must be safe
// for concurrent use. A nil clock panics.
func WithClock(now func() time.Time) Option {
	if now == nil {
		panic("scheduler clock must not be nil")
	}
	return func(options *schedulerOptions) { options.now = now }
}

// DefaultMaxTaskAttempts includes the initial assignment and two retries.
const DefaultMaxTaskAttempts = 3

// WithMaxTaskAttempts sets FIFO's per-task assignment limit within a stage attempt.
// One disables retries. It panics if max is not positive.
func WithMaxTaskAttempts(max int) Option {
	if max <= 0 {
		panic("max task attempts must be positive")
	}
	return func(options *schedulerOptions) { options.maxTaskAttempts = max }
}

func resolveSchedulerOptions(options []Option) schedulerOptions {
	config := schedulerOptions{maxTaskAttempts: DefaultMaxTaskAttempts, now: time.Now}
	for _, option := range options {
		option(&config)
	}
	return config
}

// WithLogger enables structured lifecycle events. Nil disables logging.
func WithLogger(logger *slog.Logger) Option {
	return func(options *schedulerOptions) { options.logger = logger }
}

func schedulerLogger(component string, options []Option) *slog.Logger {
	config := resolveSchedulerOptions(options)
	return config.loggerFor(component)
}

func (config schedulerOptions) loggerFor(component string) *slog.Logger {
	if config.logger == nil {
		config.logger = slog.New(slog.DiscardHandler)
	}
	return config.logger.With("component", component)
}

func attemptLogFields(jobID plan.JobID, stageID plan.StageID, attempt TaskAttemptIdentity, partitionID plan.PartitionID, workerID plan.WorkerID) []any {
	return []any{"job_id", jobID, "stage_id", stageID, "stage_attempt_id", attempt.StageAttemptID,
		"task_id", attempt.TaskID, "attempt_id", attempt.ID, "partition_id", partitionID, "worker_id", workerID}
}
