package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func inputFailure(sub dagSubmission, partition int) TaskAttemptFailure {
	r := successFor(sub.set, partition, TaskOutput{})
	return TaskAttemptFailure{JobID: r.JobID, StageID: r.StageID, Attempt: r.Attempt, PartitionID: r.PartitionID,
		Kind: FailureShuffleInput, Error: "missing bucket", ShuffleInput: &shuffle.InputReference{
			Attempt: sub.set.ShuffleInputs.Outputs[0].Attempt, PartitionID: r.PartitionID}}
}

func startRecoveryJob(t *testing.T, options ...Option) (*DAGScheduler, <-chan dagSubmission, <-chan jobCompletion, context.CancelFunc) {
	t.Helper()
	submissions := make(chan dagSubmission, 8)
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, controlledStages(submissions), options...)
	t.Cleanup(dag.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	graph, target := shuffleGraph(t)
	done := make(chan jobCompletion, 1)
	go func() {
		result, err := dag.Run(ctx, graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		done <- jobCompletion{result: result, err: err}
	}()
	return dag, submissions, done, cancel
}

func completeMaps(sub dagSubmission, run string) {
	for p := range sub.set.Tasks {
		sub.observer.TaskSucceeded(mapSuccess(sub.set, p, run))
	}
}

func TestDAGRecoveryDiscardsPartialResultsAndFencesOldEvents(t *testing.T) {
	for _, simultaneous := range []bool{false, true} {
		t.Run(fmt.Sprint(simultaneous), func(t *testing.T) {
			dag, submissions, done, _ := startRecoveryJob(t)
			run := NewRunID()
			maps := dagReceive(t, submissions)
			completeMaps(maps, run)
			result := dagReceive(t, submissions)
			if simultaneous {
				var wg sync.WaitGroup
				for p := range 2 {
					wg.Add(1)
					go func() { defer wg.Done(); result.observer.TaskFailed(inputFailure(result, p)) }()
				}
				wg.Wait()
			} else {
				result.observer.TaskSucceeded(successFor(result.set, 1, TaskOutput{Count: 999}))
				result.observer.TaskFailed(inputFailure(result, 0))
			}
			replacement := dagReceive(t, submissions)
			if replacement.set.StageID != maps.set.StageID || replacement.set.StageAttemptID == maps.set.StageAttemptID || !reflect.DeepEqual(replacement.set.Tasks, maps.set.Tasks) {
				t.Fatal("map restart changed logical work or reused its stage attempt")
			}
			// Old failures/successes and scheduling returns must not touch replacements.
			result.observer.TaskFailed(inputFailure(result, 0))
			result.observer.TaskSucceeded(successFor(result.set, 0, TaskOutput{Count: 999}))
			maps.observer.TaskSucceeded(mapSuccess(maps.set, 0, run))
			for _, sub := range []dagSubmission{maps, result} {
				if err := dag.loop.send(context.Background(), taskSetFinished{jobID: sub.set.JobID, stageID: sub.set.StageID, stageAttemptID: sub.set.StageAttemptID, err: errors.New("late old return")}); err != nil {
					t.Fatal(err)
				}
			}
			replacement.observer.TaskSucceeded(mapSuccess(replacement.set, 0, run))
			drainDAG(t, dag)
			select {
			case extra := <-submissions:
				t.Fatalf("reducer dispatched before replacement barrier: %+v", extra.set)
			default:
			}
			replacement.observer.TaskSucceeded(mapSuccess(replacement.set, 1, run))
			freshResult := dagReceive(t, submissions)
			if freshResult.set.StageAttemptID == result.set.StageAttemptID || !reflect.DeepEqual(freshResult.set.Tasks, result.set.Tasks) {
				t.Fatal("result restart changed logical work or reused its stage attempt")
			}
			if freshResult.set.ShuffleInputs.StageAttemptID != replacement.set.StageAttemptID || freshResult.set.ShuffleInputs.RunID != run {
				t.Fatal("snapshot did not use replacement map outputs")
			}
			for p := range freshResult.set.Tasks {
				freshResult.observer.TaskSucceeded(successFor(freshResult.set, p, TaskOutput{Count: 1}))
			}
			got := dagReceive(t, done)
			if got.err != nil || got.result.Count != 2 {
				t.Fatalf("recovered job = %+v", got)
			}
		})
	}
}

func TestDAGRecoveryBudgetIsSeparateAndBounded(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			_, submissions, done, _ := startRecoveryJob(t, WithMaxStageAttempts(limit))
			run := NewRunID()
			for range limit {
				maps := dagReceive(t, submissions)
				completeMaps(maps, run)
				result := dagReceive(t, submissions)
				result.observer.TaskFailed(inputFailure(result, 0))
			}
			got := dagReceive(t, done)
			if got.err == nil || !strings.Contains(got.err.Error(), fmt.Sprintf("exhausted its %d stage attempts", limit)) {
				t.Fatalf("budget result = %+v", got)
			}
			select {
			case <-submissions:
				t.Fatal("stage budget exceeded")
			default:
			}
		})
	}
}

func TestDAGRejectsUnacceptedShuffleFailureReferences(t *testing.T) {
	for _, field := range []string{"missing", "run", "job", "stage attempt", "task attempt", "map partition", "reduce partition"} {
		t.Run(field, func(t *testing.T) {
			_, submissions, done, _ := startRecoveryJob(t)
			completeMaps(dagReceive(t, submissions), NewRunID())
			result := dagReceive(t, submissions)
			failure := inputFailure(result, 0)
			switch field {
			case "missing":
				failure.ShuffleInput = nil
			case "run":
				failure.ShuffleInput.Attempt.RunID = NewRunID()
			case "job":
				failure.ShuffleInput.Attempt.JobID++
			case "stage attempt":
				failure.ShuffleInput.Attempt.StageAttemptID++
			case "task attempt":
				failure.ShuffleInput.Attempt.TaskAttemptID++
			case "map partition":
				failure.ShuffleInput.Attempt.MapPartitionID = 99
			case "reduce partition":
				failure.ShuffleInput.PartitionID++
			}
			result.observer.TaskFailed(failure)
			if got := dagReceive(t, done); got.err == nil {
				t.Fatal("invalid input failure recovered")
			}
			select {
			case <-submissions:
				t.Fatal("invalid reference restarted map stage")
			default:
			}
		})
	}
}

func TestDAGRecoveryCancellationAndShutdown(t *testing.T) {
	for _, closeScheduler := range []bool{false, true} {
		t.Run(fmt.Sprint(closeScheduler), func(t *testing.T) {
			dag, submissions, done, cancel := startRecoveryJob(t)
			completeMaps(dagReceive(t, submissions), NewRunID())
			result := dagReceive(t, submissions)
			result.observer.TaskFailed(inputFailure(result, 0))
			_ = dagReceive(t, submissions) // Replacement map execution is now active.
			want := context.Canceled
			if closeScheduler {
				want = ErrSchedulerClosed
				dag.Close()
			} else {
				cancel()
			}
			if got := dagReceive(t, done); !errors.Is(got.err, want) {
				t.Fatalf("completion = %v", got.err)
			}
		})
	}
}

func TestMaxStageAttemptsMustBePositive(t *testing.T) {
	for _, max := range []int{0, -1} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid budget accepted")
				}
			}()
			WithMaxStageAttempts(max)
		})
	}
}

func TestDAGRecoveryKeepsLiveReservationsUntilOldReportsArrive(t *testing.T) {
	physical := NewFIFOTaskScheduler(WithMaxTaskAttempts(1))
	defer physical.Close()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
	defer dag.Close()
	if err := physical.RegisterWorker("a", 2); err != nil {
		t.Fatal(err)
	}
	graph, target := shuffleGraph(t)
	done := make(chan jobCompletion, 1)
	go func() {
		r, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		done <- jobCompletion{result: r, err: err}
	}()
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 1 })
	maps := fifoOffer(t, physical, "a", 2)
	completeMap := func(a TaskAssignment) {
		t.Helper()
		set := TaskSet{JobID: a.JobID, StageID: a.StageID, StageAttemptID: a.Attempt.Identity.StageAttemptID, Tasks: []Task{maps[0].Attempt.Task, maps[1].Attempt.Task}}
		r := mapSuccess(set, int(a.Attempt.Task.PartitionID), a.RunID)
		r.Attempt, r.WorkerID = a.Attempt.Identity, a.Attempt.WorkerID
		r.Output.ShuffleOutput.Attempt.TaskAttemptID = a.Attempt.Identity.ID
		if err := physical.ReportSuccess(r); err != nil {
			t.Fatal(err)
		}
	}
	for _, a := range maps {
		completeMap(a)
	}
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 2 })
	reducers := fifoOffer(t, physical, "a", 2)
	r := failureFor(reducers[0])
	r.Kind = FailureShuffleInput
	r.ShuffleInput = &shuffle.InputReference{Attempt: reducers[0].ShuffleInputs.Outputs[0].Attempt, PartitionID: r.PartitionID}
	if err := physical.ReportFailure(r); err != nil {
		t.Fatal(err)
	}
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 3 })
	assertReserved(t, physical, "a", 1)
	first := fifoOffer(t, physical, "a", 2)
	if len(first) != 1 || first[0].Attempt.Task.StageKind != StageShuffleMap {
		t.Fatalf("replacement overcommitted live worker: %+v", first)
	}
	assertReserved(t, physical, "a", 2)
	if more := fifoOffer(t, physical, "a", 2); len(more) != 0 {
		t.Fatal("old reducer's slot reused")
	}
	if err := physical.ReportSuccess(fifoSuccess(reducers[1], 999)); err != nil {
		t.Fatal(err)
	}
	completeMap(first[0])
	second := fifoOffer(t, physical, "a", 2)
	if len(second) != 1 || second[0].Attempt.Task.StageKind != StageShuffleMap {
		t.Fatal(second)
	}
	completeMap(second[0])
	waitFIFOState(t, physical, func() bool { return len(physical.sets) == 4 })
	fresh := fifoOffer(t, physical, "a", 2)
	if len(fresh) != 2 {
		t.Fatal(fresh)
	}
	for _, a := range fresh {
		if a.Attempt.Identity.StageAttemptID == reducers[0].Attempt.Identity.StageAttemptID || a.Attempt.Identity.ID == reducers[0].Attempt.Identity.ID {
			t.Fatal("recovery reused an attempt")
		}
		fifoComplete(t, physical, a)
	}
	got := dagReceive(t, done)
	if got.err != nil || got.result.Count != 2 {
		t.Fatalf("recovered output = %+v", got)
	}
	assertReserved(t, physical, "a", 0)
}
