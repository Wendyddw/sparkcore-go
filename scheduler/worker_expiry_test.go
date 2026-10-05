package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

type expiryClock struct{ nanos atomic.Int64 }

func (c *expiryClock) now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *expiryClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }
func expiryScheduler(t *testing.T, options ...Option) (*FIFOTaskScheduler, *expiryClock) {
	t.Helper()
	c := &expiryClock{}
	c.nanos.Store(time.Unix(1700000000, 0).UnixNano())
	s := NewFIFOTaskScheduler(append(options, WithClock(c.now))...)
	t.Cleanup(s.Close)
	return s, c
}
func expire(t *testing.T, s *FIFOTaskScheduler, c *expiryClock) []plan.WorkerID {
	t.Helper()
	lost, err := s.ExpireWorkers(c.now(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return lost
}

func TestWorkerWithoutFirstHeartbeatExpires(t *testing.T) {
	s, c := expiryScheduler(t)
	if err := s.RegisterWorker("idle", 1); err != nil {
		t.Fatal(err)
	}
	c.advance(10 * time.Second)
	if ids := expire(t, s, c); !reflect.DeepEqual(ids, []plan.WorkerID{"idle"}) {
		t.Fatal(ids)
	}
}

func TestWorkerLivenessStartsAtRegistrationAndRefreshesOnlyOnValidOffers(t *testing.T) {
	s, c := expiryScheduler(t)
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	first, _ := s.Worker("a")
	if first.Status != WorkerAlive || !first.LastHeartbeat.Equal(c.now()) {
		t.Fatalf("registration = %+v", first)
	}
	c.advance(9 * time.Second)
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OfferResources("a", 2, nil); !errors.Is(err, ErrInvalidResourceOffer) {
		t.Fatal(err)
	}
	unchanged, _ := s.Worker("a")
	if !reflect.DeepEqual(first, unchanged) {
		t.Fatal("registration/invalid offer extended liveness")
	}
	if ids := expire(t, s, c); len(ids) != 0 {
		t.Fatal("expired before deadline")
	}
	fifoOffer(t, s, "a", 1)
	refreshed, _ := s.Worker("a")
	if !refreshed.LastHeartbeat.Equal(c.now()) {
		t.Fatal("valid heartbeat did not refresh liveness")
	}
	c.advance(10*time.Second - time.Nanosecond)
	if ids := expire(t, s, c); len(ids) != 0 {
		t.Fatal("expired before refreshed deadline")
	}
	c.advance(time.Nanosecond)
	if ids := expire(t, s, c); !reflect.DeepEqual(ids, []plan.WorkerID{"a"}) {
		t.Fatal(ids)
	}
	lost, _ := s.Worker("a")
	if lost.Status != WorkerLost || lost.LastHeartbeat != refreshed.LastHeartbeat {
		t.Fatal(lost)
	}
	if ids := expire(t, s, c); len(ids) != 0 {
		t.Fatal("expired twice")
	}
	if err := s.RegisterWorker("a", 1); !errors.Is(err, ErrWorkerLost) {
		t.Fatal(err)
	}
	if _, err := s.OfferResources("a", 1, nil); !errors.Is(err, ErrWorkerLost) {
		t.Fatal(err)
	}
	after, _ := s.Worker("a")
	if !reflect.DeepEqual(lost, after) {
		t.Fatal("lost worker revived")
	}
	if _, err := s.ExpireWorkers(c.now(), 0); err == nil {
		t.Fatal("invalid timeout accepted")
	}
	s.Close()
	if _, err := s.ExpireWorkers(c.now(), time.Second); !errors.Is(err, ErrTaskSchedulerClosed) {
		t.Fatal(err)
	}
}

func TestWorkerLossReassignsAtOriginalPriorityAndFencesLateReports(t *testing.T) {
	s, c := expiryScheduler(t)
	for id, slots := range map[plan.WorkerID]int{"a": 2, "b": 1} {
		if err := s.RegisterWorker(id, slots); err != nil {
			t.Fatal(err)
		}
	}
	o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0, 1))
	first := fifoOffer(t, s, "a", 2)
	fifoComplete(t, s, first[1])
	_, newerDone := startFIFOSet(t, s, context.Background(), fifoSet(2, 0))
	c.advance(9 * time.Second)
	fifoOffer(t, s, "b", 0) // Keep the survivor alive without claiming work yet.
	c.advance(time.Second)
	if ids := expire(t, s, c); !reflect.DeepEqual(ids, []plan.WorkerID{"a"}) {
		t.Fatal(ids)
	}
	assertReserved(t, s, "a", 0)
	retry := fifoOffer(t, s, "b", 1)
	if len(retry) != 1 || retry[0].JobID != 1 || retry[0].Attempt.Task.PartitionID != 0 || retry[0].Attempt.Identity.ID == first[0].Attempt.Identity.ID || retry[0].Attempt.Identity.StageAttemptID != first[0].Attempt.Identity.StageAttemptID {
		t.Fatalf("replacement = %+v", retry)
	}
	for _, old := range first {
		if err := s.ReportSuccess(fifoSuccess(old, 999)); err != nil {
			t.Fatal(err)
		}
		if err := s.ReportFailure(failureFor(old)); err != nil {
			t.Fatal(err)
		}
	}
	bad := fifoSuccess(first[0], 999)
	bad.JobID++
	if err := s.ReportSuccess(bad); !errors.Is(err, ErrMismatchedReport) {
		t.Fatal(err)
	}
	unknown := fifoSuccess(first[0], 999)
	unknown.Attempt.ID = 999
	if err := s.ReportSuccess(unknown); !errors.Is(err, ErrUnknownAttempt) {
		t.Fatal(err)
	}
	assertReserved(t, s, "b", 1)
	if ids := expire(t, s, c); len(ids) != 0 {
		t.Fatal("duplicate expiry")
	}
	fifoComplete(t, s, retry[0])
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	if len(o.failures) != 0 || len(o.successes) != 2 {
		t.Fatal("loss notified DAG before exhaustion or duplicated output")
	}
	for range 2 {
		if r := <-o.successes; r.Output.Count != 1 {
			t.Fatal("stale output accepted")
		}
	}
	next := fifoOffer(t, s, "b", 1)
	if len(next) != 1 || next[0].JobID != 2 {
		t.Fatal(next)
	}
	fifoComplete(t, s, next[0])
	if err := fifoWait(t, newerDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerLossConsumesSameAttemptBudget(t *testing.T) {
	for _, initialFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(initialFailure), func(t *testing.T) {
			s, c := expiryScheduler(t, WithMaxTaskAttempts(2))
			o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
			for n := range 2 {
				id := plan.WorkerID(fmt.Sprintf("worker-%d", n))
				if err := s.RegisterWorker(id, 1); err != nil {
					t.Fatal(err)
				}
				a := fifoOffer(t, s, id, 1)[0]
				if initialFailure && n == 0 {
					if err := s.ReportFailure(failureFor(a)); err != nil {
						t.Fatal(err)
					}
				} else {
					c.advance(10 * time.Second)
					expire(t, s, c)
				}
			}
			if err := fifoWait(t, done); err == nil {
				t.Fatal("loss budget did not terminate job")
			}
			if len(o.failures) != 1 || (<-o.failures).Kind != FailureWorkerLost {
				t.Fatal("expected one terminal scheduler-owned loss failure")
			}
			if err := ValidateFailure(FailureWorkerLost, nil); err == nil {
				t.Fatal("workers may not claim worker loss")
			}
		})
	}
}

func TestWorkerExpiryDoesNotRequeueCanceledOrTerminalSets(t *testing.T) {
	for _, cancelJob := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelJob), func(t *testing.T) {
			s, c := expiryScheduler(t)
			if err := s.RegisterWorker("a", 2); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o, done := startFIFOSet(t, s, ctx, fifoSet(1, 0, 1, 2))
			a := fifoOffer(t, s, "a", 2)
			if cancelJob {
				cancel()
			} else {
				r := failureFor(a[0])
				r.Kind = FailurePermanent
				if err := s.ReportFailure(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := fifoWait(t, done); err == nil {
				t.Fatal("expected terminal set")
			}
			c.advance(10 * time.Second)
			expire(t, s, c)
			assertReserved(t, s, "a", 0)
			if err := s.RegisterWorker("b", 1); err != nil {
				t.Fatal(err)
			}
			if a := fifoOffer(t, s, "b", 1); len(a) != 0 {
				t.Fatal("terminal work requeued")
			}
			if len(o.successes) != 0 || (cancelJob && len(o.failures) != 0) {
				t.Fatal("expiry emitted obsolete reports")
			}
		})
	}
}

func TestWorkerExpiryRacesWithHeartbeatAndSuccess(t *testing.T) {
	for range 20 {
		s, c := expiryScheduler(t)
		if err := s.RegisterWorker("a", 1); err != nil {
			t.Fatal(err)
		}
		o, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
		a := fifoOffer(t, s, "a", 1)[0]
		c.advance(10 * time.Second)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			if _, err := s.ExpireWorkers(c.now(), 10*time.Second); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := s.OfferResources("a", 0, []plan.TaskAttemptID{a.Attempt.Identity.ID}); err != nil && !errors.Is(err, ErrWorkerLost) {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := s.ReportSuccess(fifoSuccess(a, 1)); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		if err := s.RegisterWorker("b", 1); err != nil {
			t.Fatal(err)
		}
		for _, retry := range fifoOffer(t, s, "b", 1) {
			fifoComplete(t, s, retry)
		}
		if err := fifoWait(t, done); err != nil {
			t.Fatal(err)
		}
		if len(o.successes) != 1 || len(o.failures) != 0 {
			t.Fatal("race changed accepted result count")
		}
		assertReserved(t, s, "a", 0)
		assertReserved(t, s, "b", 0)
	}
}
