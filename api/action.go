package api

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/executor"
)

// Collect triggers execution and returns every record in the RDD.
func (r RDD) Collect(ctx context.Context) ([]executor.Record, error) {
	if r.ctx == nil {
		return nil, fmt.Errorf("collect RDD %d: missing context", r.id)
	}
	if r.ctx.runner == nil {
		return nil, fmt.Errorf("collect RDD %d: action runner is not configured", r.id)
	}

	records, err := r.ctx.runner.Collect(ctx, r.ctx.graph, r.id)
	if err != nil {
		return nil, fmt.Errorf("collect RDD %d: %w", r.id, err)
	}
	return records, nil
}

// Count triggers execution and returns the number of records in the RDD.
func (r RDD) Count(ctx context.Context) (int64, error) {
	if r.ctx == nil {
		return 0, fmt.Errorf("count RDD %d: missing context", r.id)
	}
	if r.ctx.runner == nil {
		return 0, fmt.Errorf("count RDD %d: action runner is not configured", r.id)
	}

	count, err := r.ctx.runner.Count(ctx, r.ctx.graph, r.id)
	if err != nil {
		return 0, fmt.Errorf("count RDD %d: %w", r.id, err)
	}
	return count, nil
}
