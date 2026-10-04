package scheduler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func TestFIFOValidatesMapOutputBeforeReleasingReservation(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	set := fifoSet(1, 0)
	set.Tasks[0].StageKind = StageShuffleMap
	set.Tasks[0].FinalAction = nil
	set.Tasks[0].ShuffleWrite = &ShuffleWriteSpec{ShuffleID: 3, Partitioner: plan.HashPartitioner(1), AggregatorID: "sum"}
	o, done := startFIFOSet(t, s, context.Background(), set)
	a := fifoOffer(t, s, "a", 1)[0]
	m := shuffle.MapOutput{Version: 1, Attempt: shuffle.AttemptIdentity{RunID: a.RunID, JobID: a.JobID, ShuffleID: 3, StageID: a.StageID, StageAttemptID: a.Attempt.Identity.StageAttemptID, TaskID: a.Attempt.Task.ID, TaskAttemptID: a.Attempt.Identity.ID}, NumMapPartitions: 1, NumReducePartitions: 1,
		Buckets: []shuffle.BucketMetadata{{SHA256: fmt.Sprintf("%x", sha256.Sum256(nil))}}}
	for _, output := range []TaskOutput{{Count: 0}, {ShuffleOutput: &m, Records: []any{}}, {ShuffleOutput: func() *shuffle.MapOutput { bad := m; bad.Attempt.RunID = NewRunID(); return &bad }()}} {
		r := fifoSuccess(a, 0)
		r.Output = output
		if err := s.ReportSuccess(r); !errors.Is(err, ErrInvalidTaskReport) {
			t.Fatalf("bad output: %v", err)
		}
		worker, _ := s.Worker("a")
		if worker.ReservedSlots != 1 || len(o.successes) != 0 {
			t.Fatal("invalid output changed task state")
		}
	}
	r := fifoSuccess(a, 0)
	r.Output.ShuffleOutput = &m
	if err := s.ReportSuccess(r); err != nil {
		t.Fatal(err)
	}
	m.Buckets[0].SHA256 = "mutated after reporting"
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	accepted := <-o.successes
	if err := accepted.Output.ShuffleOutput.Validate(); err != nil {
		t.Fatal("accepted descriptor aliases report")
	}
	accepted.Output.ShuffleOutput.Buckets[0].SHA256 = "mutated by observer"
	if err := s.attempts[a.Attempt.Identity.ID].task.output.ShuffleOutput.Validate(); err != nil {
		t.Fatal("observer aliases stored output")
	}
	if err := s.ReportSuccess(fifoSuccess(a, 999)); err != nil {
		t.Fatalf("duplicate should be ignored: %v", err)
	}
}

func TestFailureClassificationPreservesInputIdentity(t *testing.T) {
	input := &shuffle.InputError{Attempt: shuffle.AttemptIdentity{RunID: NewRunID()}, Err: fs.ErrNotExist}
	for _, test := range []struct {
		err  error
		kind FailureKind
	}{
		{input, FailureShuffleInput}, {fs.ErrNotExist, FailurePermanent}, {shuffle.ErrRecordTooLarge, FailurePermanent},
		{errors.New("user function failed"), FailureExecution}, {context.Canceled, FailureCanceled},
		{PermanentFailure(errors.New("invalid configuration")), FailurePermanent},
		{PermanentFailure(input), FailureShuffleInput}, {PermanentFailure(context.Canceled), FailureCanceled},
	} {
		kind, ref := ClassifyFailure(context.Background(), fmt.Errorf("wrapped: %w", test.err))
		if kind != test.kind {
			t.Fatalf("kind %s, want %s", kind, test.kind)
		}
		if err := ValidateFailure(kind, ref); err != nil {
			t.Fatal(err)
		}
		if kind == FailureShuffleInput && ref.Attempt != input.Attempt {
			t.Fatal("input identity lost")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("shutdown"))
	if kind, ref := ClassifyFailure(ctx, input); kind != FailureCanceled || ref != nil {
		t.Fatal("cancellation did not take precedence")
	}
}

func TestPermanentFailurePreservesCause(t *testing.T) {
	cause := errors.New("invalid record shape")
	if err := PermanentFailure(cause); !errors.Is(err, cause) || err.Error() != cause.Error() {
		t.Fatalf("lost cause: %v", err)
	}
	if PermanentFailure(nil) != nil {
		t.Fatal("nil error became a failure")
	}
}

func TestFIFOReduceSnapshotCopiesAndFailureBinding(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	output := shuffle.MapOutput{Version: 1, Attempt: shuffle.AttemptIdentity{RunID: s.runID, JobID: 1, StageID: 9}, NumMapPartitions: 1, NumReducePartitions: 1,
		Buckets: []shuffle.BucketMetadata{{SHA256: fmt.Sprintf("%x", sha256.Sum256(nil))}}}
	set := fifoSet(1, 0)
	set.ShuffleInputs = &shuffle.InputSnapshot{RunID: s.runID, JobID: 1, StageID: 9, NumMapPartitions: 1, NumReducePartitions: 1, Outputs: []shuffle.MapOutput{output}}
	o, done := startFIFOSet(t, s, context.Background(), set)
	set.ShuffleInputs.Outputs[0].Buckets[0].SHA256 = "mutated source"
	a := fifoOffer(t, s, "a", 1)[0]
	if err := a.ShuffleInputs.Validate(); err != nil {
		t.Fatal("submission did not own a snapshot copy")
	}
	a.ShuffleInputs.Outputs[0].Attempt.TaskAttemptID++
	if err := s.attempts[a.Attempt.Identity.ID].assignment.ShuffleInputs.Validate(); err != nil {
		t.Fatal(err)
	}
	failure := failureFor(a)
	failure.Kind = FailureShuffleInput
	failure.ShuffleInput = &shuffle.InputReference{Attempt: a.ShuffleInputs.Outputs[0].Attempt}
	if err := s.ReportFailure(failure); !errors.Is(err, ErrInvalidTaskReport) {
		t.Fatalf("unassigned input failure accepted: %v", err)
	}
	worker, _ := s.Worker("a")
	if worker.ReservedSlots != 1 {
		t.Fatal("rejected failure released reservation")
	}
	failure.ShuffleInput.Attempt = output.Attempt
	if err := s.ReportFailure(failure); err != nil {
		t.Fatal(err)
	}
	failure.ShuffleInput.Attempt.TaskAttemptID++
	if err := fifoWait(t, done); err == nil {
		t.Fatal("failure did not terminate set")
	}
	accepted := <-o.failures
	if accepted.ShuffleInput.Attempt != output.Attempt {
		t.Fatal("failure report aliases caller metadata")
	}
}

func TestResultOutputMustMatchAssignedAction(t *testing.T) {
	for _, action := range []ActionKind{ActionCount, ActionCollect} {
		e := TaskExecution{Task: Task{StageKind: StageResult, FinalAction: &ActionSpec{Kind: action}}}
		valid := TaskOutput{}
		if action == ActionCollect {
			valid.Records = []any{}
		}
		if err := valid.ValidateFor(e); err != nil {
			t.Fatal(err)
		}
		invalid := []TaskOutput{{Count: -1}, {ShuffleOutput: &shuffle.MapOutput{}}, {Count: 1, Records: []any{1}}}
		if action == ActionCollect {
			invalid = append(invalid, TaskOutput{})
		} else {
			invalid = append(invalid, TaskOutput{Records: []any{}})
		}
		for _, output := range invalid {
			if err := output.ValidateFor(e); err == nil {
				t.Fatalf("%s accepted %+v", action, output)
			}
		}
	}
}
