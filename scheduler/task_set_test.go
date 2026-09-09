package scheduler

import (
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestTaskSetPreservesLogicalPartitionOrder(t *testing.T) {
	tasks := []Task{
		{ID: 4, StageID: 2, PartitionID: 0},
		{ID: 5, StageID: 2, PartitionID: 1},
		{ID: 6, StageID: 2, PartitionID: 2},
	}
	taskSet := TaskSet{
		JobID:          7,
		StageID:        2,
		StageAttemptID: 1,
		Tasks:          tasks,
	}

	wantPartitions := []plan.PartitionID{0, 1, 2}
	gotPartitions := make([]plan.PartitionID, len(taskSet.Tasks))
	for i, task := range taskSet.Tasks {
		gotPartitions[i] = task.PartitionID
	}
	if !reflect.DeepEqual(gotPartitions, wantPartitions) {
		t.Fatalf("task-set partitions = %v, want %v", gotPartitions, wantPartitions)
	}
	if taskSet.JobID != 7 || taskSet.StageID != 2 || taskSet.StageAttemptID != 1 {
		t.Fatalf("task-set identity = %#v, want job 7 stage 2 attempt 1", taskSet)
	}
}
