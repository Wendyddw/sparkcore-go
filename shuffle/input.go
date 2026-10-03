package shuffle

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// InputSnapshot identifies exactly one accepted output per parent map partition.
// Validation checks consistency; the scheduler is responsible for acceptance.
type InputSnapshot struct {
	RunID               string              `json:"run_id"`
	JobID               plan.JobID          `json:"job_id"`
	ShuffleID           plan.ShuffleID      `json:"shuffle_id"`
	StageID             plan.StageID        `json:"stage_id"`
	StageAttemptID      plan.StageAttemptID `json:"stage_attempt_id"`
	NumMapPartitions    int                 `json:"num_map_partitions"`
	NumReducePartitions int                 `json:"num_reduce_partitions"`
	Outputs             []MapOutput         `json:"outputs"`
}

// Validate requires complete, ordered outputs from one producing stage attempt.
func (s InputSnapshot) Validate() error {
	if !lowerHex(s.RunID, 32) || s.NumMapPartitions <= 0 || s.NumReducePartitions <= 0 {
		return fmt.Errorf("shuffle input requires a valid run ID and positive partition counts")
	}
	if len(s.Outputs) != s.NumMapPartitions {
		return fmt.Errorf("shuffle input must contain every map partition")
	}
	for i, output := range s.Outputs {
		if err := output.Validate(); err != nil {
			return fmt.Errorf("map output %d: %w", i, err)
		}
		a := output.Attempt
		if a.RunID != s.RunID || a.JobID != s.JobID || a.ShuffleID != s.ShuffleID || a.StageID != s.StageID || a.StageAttemptID != s.StageAttemptID {
			return fmt.Errorf("map output %d does not match shuffle input identity", i)
		}
		if a.MapPartitionID != plan.PartitionID(i) || output.NumMapPartitions != s.NumMapPartitions || output.NumReducePartitions != s.NumReducePartitions {
			return fmt.Errorf("map output %d does not match shuffle input partitions", i)
		}
	}
	return nil
}

// Clone copies descriptor slices for independent ownership.
func (s InputSnapshot) Clone() InputSnapshot {
	outputs := s.Outputs
	s.Outputs = append([]MapOutput(nil), outputs...)
	for i := range s.Outputs {
		s.Outputs[i].Buckets = append([]BucketMetadata(nil), outputs[i].Buckets...)
	}
	return s
}
