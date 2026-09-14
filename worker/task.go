package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func (w *Runtime) executeAndReport(ctx context.Context, assignment protocol.TaskAssignment) (reportErr error) {
	logger := w.logger.With("job_id", assignment.JobID, "stage_id", assignment.StageID,
		"stage_attempt_id", assignment.Attempt.StageAttemptID, "task_id", assignment.Attempt.TaskID,
		"attempt_id", assignment.Attempt.ID, "partition_id", assignment.Task.PartitionID)
	logger.Info("task_started", "slots", w.config.Slots)
	var taskErr error
	defer func() {
		logger.Info("task_finished", "execution_succeeded", taskErr == nil,
			"report_acknowledged", reportErr == nil, "task_error", taskErr, "report_error", reportErr)
	}()
	result, taskErr := w.runner.RunTask(ctx, assignment.Task)
	var output protocol.TaskOutput
	if taskErr == nil {
		output, taskErr = encodeOutput(assignment.Task.FinalAction.Kind, result)
	}
	if taskErr == nil && ctx.Err() != nil {
		taskErr = ctx.Err()
	}
	// Canceled tasks still get a bounded opportunity to release remote reservations.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.config.RequestTimeout)
	defer cancel()
	var ack protocol.TaskReportResponse
	var err error
	if taskErr != nil {
		ack, err = w.client.ReportFailure(reportCtx, protocol.TaskFailureRequest{
			JobID: assignment.JobID, StageID: assignment.StageID, WorkerID: assignment.WorkerID,
			Attempt: assignment.Attempt, PartitionID: assignment.Task.PartitionID, Error: taskErr.Error(),
		})
	} else {
		ack, err = w.client.ReportSuccess(reportCtx, protocol.TaskSuccessRequest{
			JobID: assignment.JobID, StageID: assignment.StageID, WorkerID: assignment.WorkerID,
			Attempt: assignment.Attempt, PartitionID: assignment.Task.PartitionID, Output: output,
		})
	}
	if err != nil {
		return err
	}
	if err := ack.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResponse, err)
	}
	return nil
}

func encodeOutput(action scheduler.ActionKind, result scheduler.TaskOutput) (protocol.TaskOutput, error) {
	var output protocol.TaskOutput
	switch action {
	case scheduler.ActionCount:
		output.Count = result.Count
	case scheduler.ActionCollect:
		output.Records = make([]json.RawMessage, len(result.Records))
		for i, record := range result.Records {
			data, err := json.Marshal(record)
			if err != nil {
				return protocol.TaskOutput{}, fmt.Errorf("encode record %d: %w", i, err)
			}
			output.Records[i] = data
		}
	default:
		return protocol.TaskOutput{}, fmt.Errorf("unsupported task action %q", action)
	}
	return output, output.Validate()
}
