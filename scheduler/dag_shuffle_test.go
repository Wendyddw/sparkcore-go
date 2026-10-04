package scheduler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

type dagSubmission struct {
	set      TaskSet
	observer TaskSetObserver
	finish   chan error
}

func controlledStages(submissions chan<- dagSubmission) TaskSetScheduler {
	return taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
		finish := make(chan error, 1)
		select {
		case submissions <- dagSubmission{set, observer, finish}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-finish:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func shuffleGraph(t *testing.T) (*plan.RDDGraph, plan.RDDID) {
	t.Helper()
	graph := plan.NewRDDGraph()
	source := addPlannerNode(t, graph, plannerSourceNode(2))
	paired := addPlannerNode(t, graph, plannerNarrowNode("MapToPair", plan.OpMapToPair, "pair", source, 2))
	target := addPlannerNode(t, graph, plannerShuffleNode("ReduceByKey", "sum", paired, plan.HashPartitioner(2), 3))
	return graph, target
}

func dagReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DAG progress")
		var zero T
		return zero
	}
}

func mapSuccess(set TaskSet, partition int, run string) TaskAttemptSuccess {
	task := set.Tasks[partition]
	r := successFor(set, partition, TaskOutput{})
	m := &shuffle.MapOutput{Version: shuffle.FormatVersion, Attempt: shuffle.AttemptIdentity{
		RunID: run, JobID: set.JobID, ShuffleID: task.ShuffleWrite.ShuffleID, StageID: set.StageID,
		StageAttemptID: set.StageAttemptID, TaskID: task.ID, TaskAttemptID: r.Attempt.ID, MapPartitionID: task.PartitionID},
		NumMapPartitions: len(set.Tasks), NumReducePartitions: task.ShuffleWrite.Partitioner.NumPartitions}
	for p := range m.NumReducePartitions {
		m.Buckets = append(m.Buckets, shuffle.BucketMetadata{PartitionID: plan.PartitionID(p), SHA256: fmt.Sprintf("%x", sha256.Sum256(nil))})
	}
	r.Output.ShuffleOutput = m
	return r
}

// Receiving a subsequent event ensures all earlier events have been handled.
func drainDAG(t *testing.T, dag *DAGScheduler) {
	t.Helper()
	if err := dag.loop.send(context.Background(), taskSetFinished{jobID: ^plan.JobID(0)}); err != nil {
		t.Fatal(err)
	}
}

func TestDAGShuffleBarrierAndLateStageEvents(t *testing.T) {
	submissions := make(chan dagSubmission, 4)
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, controlledStages(submissions))
	defer dag.Close()
	graph, target := shuffleGraph(t)
	done := make(chan jobCompletion, 1)
	go func() {
		result, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCollect, TargetRDD: target})
		done <- jobCompletion{result: result, err: err}
	}()
	parent := dagReceive(t, submissions)
	if parent.set.Tasks[0].StageKind != StageShuffleMap || parent.set.ShuffleInputs != nil {
		t.Fatal("first stage was not a root map stage")
	}
	run := NewRunID()
	first := mapSuccess(parent.set, 0, run)
	parent.observer.TaskSucceeded(first)
	parent.observer.TaskSucceeded(first)
	drainDAG(t, dag)
	select {
	case <-submissions:
		t.Fatal("reducer launched before every map partition succeeded")
	default:
	}
	if dag.jobs[parent.set.JobID].stages[parent.set.StageID].remaining != 1 {
		t.Fatal("duplicate map report advanced the barrier")
	}
	first.Output.ShuffleOutput.Buckets[0].SHA256 = "changed after callback"
	parent.observer.TaskSucceeded(mapSuccess(parent.set, 1, run))
	child := dagReceive(t, submissions)
	if child.set.Tasks[0].StageKind != StageResult || child.set.StageAttemptID == parent.set.StageAttemptID {
		t.Fatal("invalid child stage identity")
	}
	inputs := child.set.ShuffleInputs
	if inputs == nil || inputs.RunID != run || inputs.StageAttemptID != parent.set.StageAttemptID || inputs.NumMapPartitions != 2 {
		t.Fatalf("invalid inputs: %+v", inputs)
	}
	if err := inputs.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, task := range child.set.Tasks {
		if task.ID < 2 {
			t.Fatal("logical task IDs were regenerated per stage")
		}
	}
	// This observer belongs to the map submission, so it cannot complete child tasks.
	parent.observer.TaskSucceeded(successFor(child.set, 0, TaskOutput{Records: []any{"bad"}}))
	parent.finish <- errors.New("late map scheduling error")
	for _, event := range []taskSetFinished{
		{jobID: child.set.JobID, stageID: parent.set.StageID, stageAttemptID: parent.set.StageAttemptID},
		{jobID: child.set.JobID, stageID: parent.set.StageID, stageAttemptID: child.set.StageAttemptID},
		{jobID: child.set.JobID, stageID: child.set.StageID, stageAttemptID: child.set.StageAttemptID + 1},
	} {
		if err := dag.loop.send(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	drainDAG(t, dag)
	select {
	case result := <-done:
		t.Fatalf("late stage event completed job: %+v", result)
	default:
	}
	child.observer.TaskSucceeded(successFor(child.set, 1, TaskOutput{Records: []any{"p1"}}))
	child.observer.TaskSucceeded(successFor(child.set, 0, TaskOutput{Records: []any{"p0"}}))
	result := dagReceive(t, done)
	if result.err != nil || !reflect.DeepEqual(result.result.Records, []any{"p0", "p1"}) {
		t.Fatalf("result=%+v", result)
	}
}

func TestDAGShuffleCancellationAndStageFailure(t *testing.T) {
	for _, phase := range []string{"map", "reduce"} {
		for _, reason := range []string{"cancel", "close", "failure", "incomplete"} {
			t.Run(phase+"/"+reason, func(t *testing.T) {
				submissions := make(chan dagSubmission, 4)
				dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, controlledStages(submissions))
				defer dag.Close()
				graph, target := shuffleGraph(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := dag.Run(ctx, graph, ActionSpec{Kind: ActionCount, TargetRDD: target}); done <- err }()
				stage := dagReceive(t, submissions)
				if phase == "reduce" {
					run := NewRunID()
					for p := range stage.set.Tasks {
						stage.observer.TaskSucceeded(mapSuccess(stage.set, p, run))
					}
					stage = dagReceive(t, submissions)
				}
				switch reason {
				case "cancel":
					cancel()
				case "close":
					dag.Close()
				case "incomplete":
					stage.finish <- nil
				case "failure":
					r := successFor(stage.set, 0, TaskOutput{})
					stage.observer.TaskFailed(TaskAttemptFailure{Kind: FailureExecution, JobID: r.JobID, StageID: r.StageID, Attempt: r.Attempt, PartitionID: r.PartitionID, Error: "failed"})
				}
				if err := dagReceive(t, done); err == nil {
					t.Fatal("failed/canceled stage completed job successfully")
				}
				dag.Close()
				select {
				case <-submissions:
					t.Fatal("launched more work after failed stage")
				default:
				}
			})
		}
	}
}

func TestDAGRejectsInconsistentMapOutput(t *testing.T) {
	for _, change := range []func(*shuffle.MapOutput){
		func(m *shuffle.MapOutput) { m.Attempt.JobID++ },
		func(m *shuffle.MapOutput) { m.Attempt.StageAttemptID++ },
		func(m *shuffle.MapOutput) { m.NumReducePartitions++ },
		func(m *shuffle.MapOutput) { m.Attempt.RunID = NewRunID() },
	} {
		physical := taskSetSchedulerFunc(func(ctx context.Context, set TaskSet, observer TaskSetObserver) error {
			run := NewRunID()
			observer.TaskSucceeded(mapSuccess(set, 0, run))
			bad := mapSuccess(set, 1, run)
			change(bad.Output.ShuffleOutput)
			observer.TaskSucceeded(bad)
			return nil
		})
		dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
		graph, target := shuffleGraph(t)
		_, err := dag.Run(context.Background(), graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		dag.Close()
		if err == nil {
			t.Fatal("inconsistent map metadata accepted")
		}
	}
}
