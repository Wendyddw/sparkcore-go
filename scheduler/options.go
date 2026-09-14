package scheduler

import (
	"log/slog"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// Option configures a scheduler at construction time.
type Option func(*schedulerOptions)

type schedulerOptions struct {
	logger *slog.Logger
}

// WithLogger enables structured lifecycle events. Nil disables logging.
func WithLogger(logger *slog.Logger) Option {
	return func(options *schedulerOptions) { options.logger = logger }
}

func schedulerLogger(component string, options []Option) *slog.Logger {
	var config schedulerOptions
	for _, option := range options {
		option(&config)
	}
	if config.logger == nil {
		config.logger = slog.New(slog.DiscardHandler)
	}
	return config.logger.With("component", component)
}

func attemptLogFields(jobID plan.JobID, stageID plan.StageID, attempt TaskAttemptIdentity, partitionID plan.PartitionID, workerID plan.WorkerID) []any {
	return []any{"job_id", jobID, "stage_id", stageID, "stage_attempt_id", attempt.StageAttemptID,
		"task_id", attempt.TaskID, "attempt_id", attempt.ID, "partition_id", partitionID, "worker_id", workerID}
}
