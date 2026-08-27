package api

import "fmt"

// ExplainLineage returns the metadata plan needed to compute this RDD.
// It does not trigger the action runner or read source data.
func (r RDD) ExplainLineage() (string, error) {
	if r.ctx == nil {
		return "", fmt.Errorf("explain RDD %d: missing context", r.id)
	}

	explanation, err := r.ctx.graph.ExplainLineage(r.id)
	if err != nil {
		return "", fmt.Errorf("explain RDD %d: %w", r.id, err)
	}
	return explanation, nil
}
