package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestFIFORetryBudget(t *testing.T) {
	for _, limit := range []int{1, 2, DefaultMaxTaskAttempts} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			var options []Option
			if limit != DefaultMaxTaskAttempts {
				options = append(options, WithMaxTaskAttempts(limit))
			}
			s := NewFIFOTaskScheduler(options...)
			defer s.Close()
			if err := s.RegisterWorker("a", 1); err != nil {
				t.Fatal(err)
			}
			o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
			var previous TaskAssignment
			for n := 1; n <= limit; n++ {
				assigned := fifoOffer(t, s, "a", 1)
				if len(assigned) != 1 {
					t.Fatalf("attempt %d: assignments = %d", n, len(assigned))
				}
				a := assigned[0]
				if n > 1 && (a.Attempt.Identity.ID <= previous.Attempt.Identity.ID || a.Attempt.Identity.TaskID != previous.Attempt.Identity.TaskID || a.Attempt.Identity.StageAttemptID != previous.Attempt.Identity.StageAttemptID) {
					t.Fatal("retry did not preserve logical identity and refresh physical identity")
				}
				if err := s.ReportFailure(failureFor(a)); err != nil {
					t.Fatal(err)
				}
				// Replayed failures never consume another attempt from the budget.
				if err := s.ReportFailure(failureFor(a)); err != nil {
					t.Fatal(err)
				}
				assertReserved(t, s, "a", 0)
				if n < limit {
					select {
					case err := <-done:
						t.Fatalf("early completion: %v", err)
					default:
					}
				}
				previous = a
			}
			if err := fifoWait(t, done); err == nil {
				t.Fatal("exhaustion returned nil")
			}
			if len(o.failures) != 1 || len(o.successes) != 0 {
				t.Fatalf("callbacks = %d failures, %d successes", len(o.failures), len(o.successes))
			}
			if r := <-o.failures; r.Attempt != previous.Attempt.Identity {
				t.Fatal("failure did not identify the exhausted attempt")
			}
			if a := fifoOffer(t, s, "a", 1); len(a) != 0 {
				t.Fatal("exhausted task assigned again")
			}
		})
	}
}

func TestFIFORetryKeepsPriorityPartitionsAndSuccessfulSiblings(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 3); err != nil {
		t.Fatal(err)
	}
	o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 2, 0, 1))
	first := fifoOffer(t, s, "a", 3) // Every task dispatched; admission position must survive.
	if len(first) != 3 {
		t.Fatal(first)
	}
	_, newerDone := startFIFOSet(t, s, context.Background(), fifoSet(2, 0))
	fifoComplete(t, s, first[1])
	for _, a := range []TaskAssignment{first[2], first[0]} {
		if err := s.ReportFailure(failureFor(a)); err != nil {
			t.Fatal(err)
		}
	}
	assigned := fifoOffer(t, s, "a", 3)
	if len(assigned) != 3 || assigned[0].JobID != 1 || assigned[0].Attempt.Task.PartitionID != 0 || assigned[1].JobID != 1 || assigned[1].Attempt.Task.PartitionID != 2 || assigned[2].JobID != 2 {
		t.Fatalf("retry order = %+v", assigned)
	}
	for _, old := range []TaskAssignment{first[0], first[2]} {
		if err := s.ReportSuccess(fifoSuccess(old, 999)); err != nil {
			t.Fatal(err)
		}
		if err := s.ReportFailure(failureFor(old)); err != nil {
			t.Fatal(err)
		}
	}
	assertReserved(t, s, "a", 3)
	if more := fifoOffer(t, s, "a", 3); len(more) != 0 {
		t.Fatal("late reports freed replacement slots")
	}
	for _, a := range assigned {
		fifoComplete(t, s, a)
	}
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	if err := fifoWait(t, newerDone); err != nil {
		t.Fatal(err)
	}
	if len(o.failures) != 0 || len(o.successes) != 3 {
		t.Fatalf("callbacks = %d failures, %d successes", len(o.failures), len(o.successes))
	}
	for range 3 {
		if r := <-o.successes; r.Output.Count != 1 {
			t.Fatal("obsolete output accepted")
		}
	}
	assertReserved(t, s, "a", 0)
}

func TestFIFONonRetryableFailures(t *testing.T) {
	for _, kind := range []FailureKind{FailurePermanent, FailureCanceled} {
		t.Run(string(kind), func(t *testing.T) {
			s := NewFIFOTaskScheduler()
			defer s.Close()
			if err := s.RegisterWorker("a", 1); err != nil {
				t.Fatal(err)
			}
			o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
			r := failureFor(fifoOffer(t, s, "a", 1)[0])
			r.Kind = kind
			if err := s.ReportFailure(r); err != nil {
				t.Fatal(err)
			}
			if err := fifoWait(t, done); err == nil {
				t.Fatal("terminal failure returned nil")
			}
			if len(o.failures) != 1 || len(fifoOffer(t, s, "a", 1)) != 0 {
				t.Fatal("nonretryable task was requeued")
			}
		})
	}
}

func TestFIFORetryCancellationAndShutdown(t *testing.T) {
	for _, mode := range []string{"cancel before failure", "cancel after requeue", "close after requeue", "sibling failure"} {
		t.Run(mode, func(t *testing.T) {
			s := NewFIFOTaskScheduler()
			defer s.Close()
			if err := s.RegisterWorker("a", 2); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o, done := startFIFOSet(t, s, ctx, fifoSet(1, 0, 1))
			a := fifoOffer(t, s, "a", 2)
			if mode == "cancel before failure" {
				cancel()
			}
			if err := s.ReportFailure(failureFor(a[0])); err != nil {
				t.Fatal(err)
			}
			want := context.Canceled
			switch mode {
			case "cancel after requeue":
				cancel()
			case "close after requeue":
				s.Close()
				want = ErrTaskSchedulerClosed
			case "sibling failure":
				r := failureFor(a[1])
				r.Kind = FailurePermanent
				if err := s.ReportFailure(r); err != nil {
					t.Fatal(err)
				}
				want = nil
			}
			if err := fifoWait(t, done); err == nil || (want != nil && !errors.Is(err, want)) {
				t.Fatalf("completion = %v, want %v", err, want)
			}
			if mode != "close after requeue" {
				if more := fifoOffer(t, s, "a", 2); len(more) != 0 {
					t.Fatal("terminal set retried")
				}
				fifoComplete(t, s, a[1])
				assertReserved(t, s, "a", 0)
			}
			if len(o.successes) != 0 || (mode != "sibling failure" && len(o.failures) != 0) {
				t.Fatal("obsolete callback delivered")
			}
		})
	}
}

func TestFIFOConcurrentRetryReportsReleaseOnce(t *testing.T) {
	s := NewFIFOTaskScheduler(WithMaxTaskAttempts(2))
	defer s.Close()
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
	old := fifoOffer(t, s, "a", 1)[0]
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.ReportFailure(failureFor(old)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	fresh := fifoOffer(t, s, "a", 1)[0]
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.ReportSuccess(fifoSuccess(old, 999)); err != nil {
				t.Error(err)
			}
			if err := s.ReportFailure(failureFor(old)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assertReserved(t, s, "a", 1)
	fifoComplete(t, s, fresh)
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	if len(o.successes) != 1 || len(o.failures) != 0 {
		t.Fatal("duplicate reports altered outcome")
	}
}

func TestMaxTaskAttemptsMustBePositive(t *testing.T) {
	for _, max := range []int{0, -1} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("nonpositive limit accepted")
				}
			}()
			WithMaxTaskAttempts(max)
		})
	}
}
