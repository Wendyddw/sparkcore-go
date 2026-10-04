package scheduler

import "fmt"

// ValidateFor checks output against the physical assignment before acceptance.
func (o TaskOutput) ValidateFor(e TaskExecution) error {
	t := e.Task
	if t.StageKind == StageShuffleMap {
		if t.ShuffleWrite == nil || t.FinalAction != nil || o.ShuffleOutput == nil || o.Records != nil || o.Count != 0 {
			return fmt.Errorf("map task requires only shuffle output")
		}
		m := o.ShuffleOutput
		if err := m.Validate(); err != nil {
			return err
		}
		a := m.Attempt
		if a.RunID != e.RunID || a.JobID != e.JobID || a.ShuffleID != t.ShuffleWrite.ShuffleID || a.StageID != t.StageID || a.StageAttemptID != e.Attempt.StageAttemptID || a.TaskID != t.ID || a.TaskAttemptID != e.Attempt.ID || a.MapPartitionID != t.PartitionID || m.NumMapPartitions != t.NumPartitions || m.NumReducePartitions != t.ShuffleWrite.Partitioner.NumPartitions {
			return fmt.Errorf("shuffle output does not match assigned map attempt")
		}
		return nil
	}
	if t.StageKind != StageResult || t.FinalAction == nil || o.ShuffleOutput != nil || o.Count < 0 {
		return fmt.Errorf("result task requires action output")
	}
	switch t.FinalAction.Kind {
	case ActionCount:
		if o.Records != nil {
			return fmt.Errorf("Count output must not contain records")
		}
	case ActionCollect:
		if o.Count != 0 || o.Records == nil {
			return fmt.Errorf("Collect output requires a records array and zero count")
		}
	default:
		return fmt.Errorf("unsupported result action %q", t.FinalAction.Kind)
	}
	return nil
}

// Clone detaches descriptor slices. Individual action records remain immutable.
func (o TaskOutput) Clone() TaskOutput {
	if o.Records != nil {
		o.Records = append([]any{}, o.Records...)
	}
	if o.ShuffleOutput != nil {
		copy := *o.ShuffleOutput
		copy.Buckets = append(copy.Buckets[:0:0], copy.Buckets...)
		o.ShuffleOutput = &copy
	}
	return o
}
