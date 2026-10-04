package shuffle

import (
	"fmt"
	"github.com/Wendyddw/sparkcore-go/plan"
)

// InputReference identifies a bucket in a published map attempt.
type InputReference struct {
	Attempt     AttemptIdentity  `json:"attempt"`
	PartitionID plan.PartitionID `json:"partition_id"`
}

func (r InputReference) Validate() error {
	if err := r.Attempt.Validate(); err != nil {
		return err
	}
	if r.PartitionID < 0 {
		return fmt.Errorf("input partition must be nonnegative")
	}
	return nil
}

// CloneInput copies an optional snapshot and its descriptor slices.
func CloneInput(input *InputSnapshot) *InputSnapshot {
	if input == nil {
		return nil
	}
	copy := input.Clone()
	return &copy
}
