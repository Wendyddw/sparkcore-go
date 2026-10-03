package shuffle

import "testing"

func TestInputSnapshotValidationAndOwnership(t *testing.T) {
	first := emptyMapOutput()
	first.NumMapPartitions = 2
	second := first
	second.Attempt.MapPartitionID = 1
	second.Attempt.TaskID = 1
	second.Attempt.TaskAttemptID = 1
	input := InputSnapshot{RunID: first.Attempt.RunID, NumMapPartitions: 2, NumReducePartitions: 2, Outputs: []MapOutput{first, second}}
	if err := input.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*InputSnapshot){
		"missing":   func(s *InputSnapshot) { s.Outputs = s.Outputs[:1] },
		"extra":     func(s *InputSnapshot) { s.Outputs = append(s.Outputs, s.Outputs[0]) },
		"reordered": func(s *InputSnapshot) { s.Outputs[0], s.Outputs[1] = s.Outputs[1], s.Outputs[0] },
		"duplicate": func(s *InputSnapshot) { s.Outputs[1] = s.Outputs[0] },
		"run":       func(s *InputSnapshot) { s.RunID = "invalid" },
		"job":       func(s *InputSnapshot) { s.Outputs[1].Attempt.JobID++ },
		"shuffle":   func(s *InputSnapshot) { s.Outputs[1].Attempt.ShuffleID++ },
		"stage":     func(s *InputSnapshot) { s.Outputs[1].Attempt.StageID++ },
		"attempt":   func(s *InputSnapshot) { s.Outputs[1].Attempt.StageAttemptID++ },
		"maps":      func(s *InputSnapshot) { s.Outputs[1].NumMapPartitions++ },
		"reduces":   func(s *InputSnapshot) { s.NumReducePartitions++ },
		"bucket":    func(s *InputSnapshot) { s.Outputs[1].Buckets[0].SHA256 = "bad" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := input.Clone()
			change(&copy)
			if err := copy.Validate(); err == nil {
				t.Fatal("invalid input accepted")
			}
			if err := input.Validate(); err != nil {
				t.Fatalf("clone mutated original: %v", err)
			}
		})
	}
}
