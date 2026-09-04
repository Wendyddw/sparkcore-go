// Package jobspec defines the declarative JSON format used by example commands.
package jobspec

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// Spec describes one source, its ordered transformations, and a final action.
type Spec struct {
	Source          SourceSpec           `json:"source"`
	Transformations []TransformationSpec `json:"transformations,omitempty"`
	Action          scheduler.ActionKind `json:"action"`
}

// SourceSpec describes a lazily opened text-file source.
type SourceSpec struct {
	Path          string `json:"path"`
	NumPartitions int    `json:"num_partitions"`
}

// TransformationSpec describes one named transformation in pipeline order.
// NumPartitions applies only to ReduceByKey.
type TransformationSpec struct {
	Kind          plan.OperatorKind `json:"kind"`
	FunctionID    string            `json:"function_id"`
	NumPartitions int               `json:"num_partitions,omitempty"`
}

// Decode reads exactly one strict JSON job specification and validates it.
func Decode(reader io.Reader) (Spec, error) {
	if reader == nil {
		return Spec{}, fmt.Errorf("decode job: reader is nil")
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var spec Spec
	if err := decoder.Decode(&spec); err != nil {
		return Spec{}, fmt.Errorf("decode job: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Spec{}, fmt.Errorf("decode job: multiple JSON values")
		}
		return Spec{}, fmt.Errorf("decode job: trailing data: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return Spec{}, err
	}
	return spec, nil
}

// Validate checks schema-level fields without reading source data or resolving functions.
func (s Spec) Validate() error {
	if s.Source.Path == "" {
		return fmt.Errorf("validate job: source path must not be empty")
	}
	if s.Source.NumPartitions <= 0 {
		return fmt.Errorf("validate job: source partition count must be positive")
	}
	for index, transformation := range s.Transformations {
		if err := transformation.validate(); err != nil {
			return fmt.Errorf("validate job transformation %d: %w", index, err)
		}
	}
	switch s.Action {
	case scheduler.ActionCollect, scheduler.ActionCount:
		return nil
	default:
		return fmt.Errorf("validate job: unsupported action %q", s.Action)
	}
}

func (s TransformationSpec) validate() error {
	switch s.Kind {
	case plan.OpMap, plan.OpFilter, plan.OpMapToPair, plan.OpMapValues:
		if s.NumPartitions != 0 {
			return fmt.Errorf("operator %q does not accept num_partitions", s.Kind)
		}
	case plan.OpReduceByKey:
		if s.NumPartitions <= 0 {
			return fmt.Errorf("operator %q requires a positive num_partitions", s.Kind)
		}
	default:
		return fmt.Errorf("unsupported operator %q", s.Kind)
	}
	if s.FunctionID == "" {
		return fmt.Errorf("operator %q requires function_id", s.Kind)
	}
	return nil
}
