package scheduler

import (
	"context"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestDAGShuffleRetriesKeepAcceptedOutputs(t *testing.T) {
	physical := NewFIFOTaskScheduler(WithMaxTaskAttempts(2))
	defer physical.Close()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
	defer dag.Close()
	if err := physical.RegisterWorker("a", 2); err != nil {
		t.Fatal(err)
	}
	graph, target := shuffleGraph(t)
	done := make(chan jobCompletion, 1)
	go func() {
		result, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		done <- jobCompletion{result: result, err: err}
	}()
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 1 })
	maps := fifoOffer(t, physical, "a", 2)
	if len(maps) != 2 {
		t.Fatal(maps)
	}
	mapReport := func(a TaskAssignment) TaskAttemptSuccess {
		set := TaskSet{JobID: a.JobID, StageID: a.StageID, StageAttemptID: a.Attempt.Identity.StageAttemptID, Tasks: []Task{maps[0].Attempt.Task, maps[1].Attempt.Task}}
		r := mapSuccess(set, int(a.Attempt.Task.PartitionID), a.RunID)
		r.WorkerID, r.Attempt = a.Attempt.WorkerID, a.Attempt.Identity
		r.Output.ShuffleOutput.Attempt.TaskAttemptID = a.Attempt.Identity.ID
		return r
	}
	if err := physical.ReportSuccess(mapReport(maps[1])); err != nil {
		t.Fatal(err)
	}
	if err := physical.ReportFailure(failureFor(maps[0])); err != nil {
		t.Fatal(err)
	}
	retries := fifoOffer(t, physical, "a", 2)
	if len(retries) != 1 || retries[0].Attempt.Task.StageKind != StageShuffleMap || retries[0].Attempt.Task.PartitionID != 0 {
		t.Fatalf("map retry = %+v", retries)
	}
	// A late publication report from the failed attempt must not satisfy the barrier.
	if err := physical.ReportSuccess(mapReport(maps[0])); err != nil {
		t.Fatal(err)
	}
	if extra := fifoOffer(t, physical, "a", 2); len(extra) != 0 {
		t.Fatal("reducers launched before replacement map succeeded")
	}
	if err := physical.ReportSuccess(mapReport(retries[0])); err != nil {
		t.Fatal(err)
	}
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 2 })
	reducers := fifoOffer(t, physical, "a", 2)
	if len(reducers) != 2 {
		t.Fatal(reducers)
	}
	for _, r := range reducers {
		inputs := r.ShuffleInputs
		if inputs == nil || inputs.Outputs[0].Attempt.TaskAttemptID != retries[0].Attempt.Identity.ID || inputs.Outputs[1].Attempt.TaskAttemptID != maps[1].Attempt.Identity.ID {
			t.Fatalf("snapshot lost accepted attempts: %+v", inputs)
		}
	}
	fifoComplete(t, physical, reducers[1])
	if err := physical.ReportFailure(failureFor(reducers[0])); err != nil {
		t.Fatal(err)
	}
	replacement := fifoOffer(t, physical, "a", 2)
	if len(replacement) != 1 || replacement[0].Attempt.Task.PartitionID != plan.PartitionID(0) {
		t.Fatalf("reduce retry = %+v", replacement)
	}
	if err := physical.ReportSuccess(fifoSuccess(reducers[0], 999)); err != nil {
		t.Fatal(err)
	}
	fifoComplete(t, physical, replacement[0])
	got := dagReceive(t, done)
	if got.err != nil || got.result.Count != 2 {
		t.Fatalf("job result = %+v", got)
	}
	assertReserved(t, physical, "a", 0)
}
