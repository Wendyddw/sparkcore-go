package coordinator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// JobService builds submitted lineage and waits for distributed action results.
// Concurrent submissions share a DAG scheduler but own separate lineage graphs.
type JobService struct {
	registry *executor.FunctionRegistry
	dag      *scheduler.DAGScheduler
}

// NewJobService starts a DAG scheduler using the supplied physical scheduler.
// Register named functions before submitting jobs. The caller owns tasks.
func NewJobService(registry *executor.FunctionRegistry, tasks scheduler.TaskSetScheduler) (*JobService, error) {
	if registry == nil {
		return nil, fmt.Errorf("function registry is nil")
	}
	if tasks == nil {
		return nil, fmt.Errorf("task scheduler is nil")
	}
	return &JobService{registry: registry, dag: scheduler.NewDAGScheduler(registry, tasks)}, nil
}

// Submit builds a lazy graph and blocks until its action succeeds or fails.
// Canceling ctx cancels scheduling; already assigned workers may still report.
func (s *JobService) Submit(ctx context.Context, spec jobspec.Spec) (protocol.JobResultResponse, error) {
	if ctx == nil {
		return protocol.JobResultResponse{}, fmt.Errorf("job context is nil")
	}
	if err := ctx.Err(); err != nil {
		return protocol.JobResultResponse{}, err
	}
	// Build this job's lazy lineage without reading the source.
	driver := api.NewContext(s.registry, executor.NewSchedulerActionRunner(s.dag))
	rdd, err := jobspec.Build(driver, spec)
	if err != nil {
		return protocol.JobResultResponse{}, fmt.Errorf("build job: %w", err)
	}
	// Trigger DAG/FIFO scheduling and wait for the action result.
	result := protocol.JobResultResponse{Action: spec.Action}
	switch spec.Action {
	case scheduler.ActionCount:
		result.Count, err = rdd.Count(ctx)
	case scheduler.ActionCollect:
		var records []executor.Record
		records, err = rdd.Collect(ctx)
		if err == nil {
			// Encode merged records for the JSON response.
			result.Records = make([]json.RawMessage, len(records))
			for i, record := range records {
				result.Records[i], err = json.Marshal(record)
				if err != nil {
					return protocol.JobResultResponse{}, fmt.Errorf("encode result record %d: %w", i, err)
				}
			}
		}
	}
	if err != nil {
		return protocol.JobResultResponse{}, fmt.Errorf("run job: %w", err)
	}
	// Return the completed result to the submission handler.
	return result, nil
}

// Close cancels jobs and stops the owned DAG scheduler. It does not close tasks.
func (s *JobService) Close() {
	s.dag.Close()
}
