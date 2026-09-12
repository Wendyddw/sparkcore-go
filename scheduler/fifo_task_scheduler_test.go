package scheduler

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

type fifoObserver struct {
	successes chan TaskAttemptSuccess
	failures  chan TaskAttemptFailure
}

func newFIFOObserver() *fifoObserver {
	return &fifoObserver{make(chan TaskAttemptSuccess, 32), make(chan TaskAttemptFailure, 32)}
}
func (o *fifoObserver) TaskSucceeded(r TaskAttemptSuccess) { o.successes <- r }
func (o *fifoObserver) TaskFailed(r TaskAttemptFailure)    { o.failures <- r }

func fifoSet(job plan.JobID, partitions ...plan.PartitionID) TaskSet {
	set := TaskSet{JobID: job, StageID: 0, StageAttemptID: plan.StageAttemptID(job)}
	for _, partition := range partitions {
		set.Tasks = append(set.Tasks, Task{ID: plan.TaskID(partition), PartitionID: partition, StageKind: StageResult, NumPartitions: len(partitions)})
	}
	return set
}

func startFIFOSet(t *testing.T, s *FIFOTaskScheduler, ctx context.Context, set TaskSet) (*fifoObserver, <-chan error) {
	t.Helper()
	observer := newFIFOObserver()
	done := make(chan error, 1)
	go func() { done <- s.ScheduleTaskSet(ctx, set, observer) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		_, exists := s.sets[taskSetKey{set.JobID, set.StageID, set.StageAttemptID}]
		s.mu.Unlock()
		if exists {
			return observer, done
		}
		select {
		case err := <-done:
			t.Fatalf("submission failed: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("task set was not queued")
		}
		runtime.Gosched()
	}
}
func fifoOffer(t *testing.T, s *FIFOTaskScheduler, id plan.WorkerID, free int, running ...plan.TaskAttemptID) []TaskAssignment {
	t.Helper()
	assignments, err := s.OfferResources(id, free, running)
	if err != nil {
		t.Fatal(err)
	}
	return assignments
}
func fifoSuccess(a TaskAssignment, count int64) TaskAttemptSuccess {
	return TaskAttemptSuccess{JobID: a.JobID, StageID: a.StageID, Attempt: a.Attempt.Identity, PartitionID: a.Attempt.Task.PartitionID, WorkerID: a.Attempt.WorkerID, Output: TaskOutput{Count: count}}
}
func fifoComplete(t *testing.T, s *FIFOTaskScheduler, a TaskAssignment) {
	t.Helper()
	if err := s.ReportSuccess(fifoSuccess(a, 1)); err != nil {
		t.Fatal(err)
	}
}
func fifoWait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("submission did not exit")
		return nil
	}
}

func TestFIFOReservesInFlightSlotsAndOrdersPartitions(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 2); err != nil {
		t.Fatal(err)
	}
	_, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 2, 0, 1))
	first := fifoOffer(t, s, "a", 2)
	if len(first) != 2 || first[0].Attempt.Task.PartitionID != 0 || first[1].Attempt.Task.PartitionID != 1 {
		t.Fatalf("assignments = %#v", first)
	}
	if got := fifoOffer(t, s, "a", 2); len(got) != 0 {
		t.Fatal("repeated heartbeat reused in-flight slots")
	}
	fifoComplete(t, s, first[0])
	fifoComplete(t, s, first[0])
	next := fifoOffer(t, s, "a", 1, first[1].Attempt.Identity.ID)
	if len(next) != 1 || next[0].Attempt.Task.PartitionID != 2 || next[0].Attempt.Identity.ID <= first[1].Attempt.Identity.ID {
		t.Fatalf("next = %#v", next)
	}
	fifoComplete(t, s, first[1])
	fifoComplete(t, s, next[0])
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOTwoWorkersChooseOldestPendingTaskSet(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	for _, id := range []plan.WorkerID{"a", "b"} {
		if err := s.RegisterWorker(id, 2); err != nil {
			t.Fatal(err)
		}
	}
	_, firstDone := startFIFOSet(t, s, context.Background(), fifoSet(1, 3, 1, 0, 2))
	_, secondDone := startFIFOSet(t, s, context.Background(), fifoSet(2, 0))
	a := fifoOffer(t, s, "a", 2)
	b := fifoOffer(t, s, "b", 2)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("assignments = %d, %d", len(a), len(b))
	}
	for i, assignment := range append(a, b...) {
		if assignment.JobID != 1 || assignment.Attempt.Task.PartitionID != plan.PartitionID(i) {
			t.Fatalf("assignment = %#v", assignment)
		}
	}
	fifoComplete(t, s, a[0])
	next := fifoOffer(t, s, "a", 1, a[1].Attempt.Identity.ID)
	if len(next) != 1 || next[0].JobID != 2 {
		t.Fatalf("next = %#v", next)
	}
	for _, assignment := range append(append(a[1:], b...), next...) {
		fifoComplete(t, s, assignment)
	}
	if err := fifoWait(t, firstDone); err != nil {
		t.Fatal(err)
	}
	if err := fifoWait(t, secondDone); err != nil {
		t.Fatal(err)
	}
}

func failureFor(a TaskAssignment) TaskAttemptFailure {
	r := fifoSuccess(a, 0)
	return TaskAttemptFailure{JobID: r.JobID, StageID: r.StageID, Attempt: r.Attempt, PartitionID: r.PartitionID, WorkerID: r.WorkerID, Error: "broken partition"}
}
func assertReserved(t *testing.T, s *FIFOTaskScheduler, id plan.WorkerID, want int) {
	t.Helper()
	snapshot, err := s.Worker(id)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReservedSlots != want {
		t.Fatalf("reserved = %d, want %d", snapshot.ReservedSlots, want)
	}
}

func TestFIFOFailureRetainsSiblingReservationsUntilReports(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 2); err != nil {
		t.Fatal(err)
	}
	observer, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0, 1, 2))
	a := fifoOffer(t, s, "a", 2)
	if err := s.ReportFailure(failureFor(a[0])); err != nil {
		t.Fatal(err)
	}
	if err := fifoWait(t, done); err == nil {
		t.Fatal("failed task set returned nil")
	}
	if err := s.ReportFailure(failureFor(a[0])); err != nil {
		t.Fatal(err)
	}
	fifoComplete(t, s, a[0])
	assertReserved(t, s, "a", 1)
	_, nextDone := startFIFOSet(t, s, context.Background(), fifoSet(2, 0, 1))
	next := fifoOffer(t, s, "a", 2)
	if len(next) != 1 || next[0].JobID != 2 {
		t.Fatalf("canceled sibling capacity reused: %#v", next)
	}
	fifoComplete(t, s, a[1]) // Obsolete result frees its reservation but emits no callback.
	if len(observer.successes) != 0 || len(observer.failures) != 1 {
		t.Fatalf("reports = %d successes, %d failures", len(observer.successes), len(observer.failures))
	}
	last := fifoOffer(t, s, "a", 1, next[0].Attempt.Identity.ID)
	if len(last) != 1 {
		t.Fatalf("assignments after sibling stopped = %d", len(last))
	}
	fifoComplete(t, s, next[0])
	fifoComplete(t, s, last[0])
	if err := fifoWait(t, nextDone); err != nil {
		t.Fatal(err)
	}
	assertReserved(t, s, "a", 0)
}

func TestFIFOCancellationUnblocksAndPreservesOccupiedSlots(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer, done := startFIFOSet(t, s, ctx, fifoSet(1, 0, 1))
	assigned := fifoOffer(t, s, "a", 1)[0]
	cancel()
	if err := fifoWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	assertReserved(t, s, "a", 1)
	fifoComplete(t, s, assigned)
	assertReserved(t, s, "a", 0)
	if len(observer.successes) != 0 {
		t.Fatal("canceled task emitted success")
	}
	if got := fifoOffer(t, s, "a", 1); len(got) != 0 {
		t.Fatal("canceled pending task was assigned")
	}
}

func TestFIFORejectsInvalidWorkersOffersAndReports(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	for _, registration := range []struct {
		id    plan.WorkerID
		slots int
	}{{"", 1}, {"a", 0}, {"a", -1}} {
		if err := s.RegisterWorker(registration.id, registration.slots); !errors.Is(err, ErrInvalidWorker) {
			t.Fatalf("invalid registration error = %v", err)
		}
	}
	if err := s.RegisterWorker("a", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterWorker("a", 2); err != nil {
		t.Fatal("identical registration must be idempotent")
	}
	if err := s.RegisterWorker("a", 3); !errors.Is(err, ErrWorkerConflict) {
		t.Fatalf("conflicting registration error = %v", err)
	}
	if _, err := s.Worker("missing"); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("unknown worker error = %v", err)
	}
	if err := s.RegisterWorker("b", 2); err != nil {
		t.Fatal(err)
	}
	_, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0, 1))
	assignments := fifoOffer(t, s, "a", 2)
	a := assignments[0]
	before, _ := s.Worker("a")
	for _, offer := range []struct {
		id      plan.WorkerID
		free    int
		running []plan.TaskAttemptID
	}{
		{"missing", 1, nil}, {"a", -1, nil}, {"a", 3, nil}, {"a", 2, []plan.TaskAttemptID{a.Attempt.Identity.ID}},
		{"a", 0, []plan.TaskAttemptID{a.Attempt.Identity.ID, a.Attempt.Identity.ID}},
		{"a", 1, []plan.TaskAttemptID{999}}, {"b", 1, []plan.TaskAttemptID{a.Attempt.Identity.ID}},
	} {
		want := ErrInvalidResourceOffer
		if offer.id == "missing" {
			want = ErrUnknownWorker
		}
		if _, err := s.OfferResources(offer.id, offer.free, offer.running); !errors.Is(err, want) {
			t.Fatalf("invalid offer %#v: error = %v, want %v", offer, err, want)
		}
	}
	after, _ := s.Worker("a")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid offer mutated worker state")
	}
	for _, mutate := range []func(*TaskAttemptSuccess){
		func(r *TaskAttemptSuccess) { r.JobID++ }, func(r *TaskAttemptSuccess) { r.StageID++ },
		func(r *TaskAttemptSuccess) { r.Attempt.ID = 999 }, func(r *TaskAttemptSuccess) { r.Attempt.StageAttemptID++ },
		func(r *TaskAttemptSuccess) { r.Attempt.TaskID++ }, func(r *TaskAttemptSuccess) { r.PartitionID++ }, func(r *TaskAttemptSuccess) { r.WorkerID = "b" },
	} {
		r := fifoSuccess(a, 100)
		mutate(&r)
		if err := s.ReportSuccess(r); err == nil {
			t.Fatalf("invalid report accepted: %#v", r)
		}
	}
	invalidFailure := failureFor(a)
	invalidFailure.Error = ""
	if err := s.ReportFailure(invalidFailure); err == nil {
		t.Fatal("empty failure accepted")
	}
	assertReserved(t, s, "a", 2)
	for _, a := range assignments {
		fifoComplete(t, s, a)
	}
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOShutdownUnblocksPendingAndRunningSubmissions(t *testing.T) {
	s := NewFIFOTaskScheduler()
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	_, running := startFIFOSet(t, s, context.Background(), fifoSet(1, 0))
	assignment := fifoOffer(t, s, "a", 1)[0]
	_, pending := startFIFOSet(t, s, context.Background(), fifoSet(2, 0))
	closed := make(chan struct{})
	go func() { s.Close(); s.Close(); close(closed) }()
	for _, done := range []<-chan error{running, pending} {
		if err := fifoWait(t, done); !errors.Is(err, ErrTaskSchedulerClosed) {
			t.Fatalf("error = %v", err)
		}
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked")
	}
	if err := s.RegisterWorker("b", 1); !errors.Is(err, ErrTaskSchedulerClosed) {
		t.Fatal(err)
	}
	if _, err := s.OfferResources("a", 1, nil); !errors.Is(err, ErrTaskSchedulerClosed) {
		t.Fatal(err)
	}
	if err := s.ReportSuccess(fifoSuccess(assignment, 1)); !errors.Is(err, ErrTaskSchedulerClosed) {
		t.Fatal(err)
	}
	if err := s.ScheduleTaskSet(context.Background(), fifoSet(3, 0), newFIFOObserver()); !errors.Is(err, ErrTaskSchedulerClosed) {
		t.Fatal(err)
	}
}

func TestFIFOConcurrentOffersAndDuplicateReports(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	for _, id := range []plan.WorkerID{"a", "b"} {
		if err := s.RegisterWorker(id, 2); err != nil {
			t.Fatal(err)
		}
	}
	observer, done := startFIFOSet(t, s, context.Background(), fifoSet(1, 0, 1, 2, 3, 4, 5, 6, 7))
	errorsCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []plan.WorkerID{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 8 {
				assignments, err := s.OfferResources(id, 2, nil)
				if err != nil {
					errorsCh <- err
					return
				}
				var reports sync.WaitGroup
				for _, a := range assignments {
					for range 2 {
						reports.Add(1)
						go func() {
							defer reports.Done()
							if err := s.ReportSuccess(fifoSuccess(a, 1)); err != nil {
								t.Error(err)
							}
						}()
					}
				}
				reports.Wait()
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	if len(observer.successes) != 8 {
		t.Fatalf("successes = %d, want 8", len(observer.successes))
	}
	ids := make(map[plan.TaskAttemptID]bool)
	for range 8 {
		r := <-observer.successes
		if ids[r.Attempt.ID] {
			t.Fatal("attempt ID reused")
		}
		ids[r.Attempt.ID] = true
	}
	assertReserved(t, s, "a", 0)
	assertReserved(t, s, "b", 0)
}

type blockingFIFOObserver struct {
	entered chan struct{}
	release chan struct{}
}

func (o *blockingFIFOObserver) TaskSucceeded(TaskAttemptSuccess) { close(o.entered); <-o.release }
func (o *blockingFIFOObserver) TaskFailed(TaskAttemptFailure)    {}

func TestFIFOCallbacksDoNotHoldLockAndCloseWaitsForThem(t *testing.T) {
	s := NewFIFOTaskScheduler()
	release := make(chan struct{})
	defer s.Close()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	observer := &blockingFIFOObserver{make(chan struct{}), release}
	done := make(chan error, 1)
	go func() { done <- s.ScheduleTaskSet(context.Background(), fifoSet(1, 0), observer) }()
	waitFIFOState(t, s, func() bool { return len(s.queue) == 1 })
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	fifoComplete(t, s, fifoOffer(t, s, "a", 1)[0])
	select {
	case <-observer.entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	// RegisterWorker can take the mutex while the observer is blocked.
	registered := make(chan error, 1)
	go func() { registered <- s.RegisterWorker("b", 1) }()
	if err := fifoWait(t, registered); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	waitFIFOState(t, s, func() bool { return s.closed })
	select {
	case <-closed:
		t.Fatal("Close returned while callback was blocked")
	default:
	}
	unblock()
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after callback returned")
	}
}

func waitFIFOState(t *testing.T, s *FIFOTaskScheduler, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		ready := predicate()
		s.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for scheduler state")
		}
		runtime.Gosched()
	}
}

func TestFIFOValidatesSubmissionsAndCopiesMetadata(t *testing.T) {
	s := NewFIFOTaskScheduler()
	defer s.Close()
	invalid := []TaskSet{fifoSet(1), fifoSet(1, 0, 0), fifoSet(1, -1)}
	mismatched := fifoSet(1, 0)
	mismatched.Tasks[0].StageID = 99
	invalid = append(invalid, mismatched)
	duplicateID := fifoSet(1, 0, 1)
	duplicateID.Tasks[1].ID = duplicateID.Tasks[0].ID
	invalid = append(invalid, duplicateID)
	for _, set := range invalid {
		if err := s.ScheduleTaskSet(context.Background(), set, newFIFOObserver()); err == nil {
			t.Fatalf("invalid set accepted: %#v", set)
		}
	}
	if err := s.ScheduleTaskSet(context.Background(), fifoSet(1, 0), nil); err == nil {
		t.Fatal("nil observer accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.ScheduleTaskSet(canceled, fifoSet(1, 0), newFIFOObserver()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	set := fifoSet(1, 0)
	set.Tasks[0].Operations = []StageOperation{{Kind: StageOperationRDD, RDD: &RDDOperationSpec{Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "original"}}}}
	observer, done := startFIFOSet(t, s, context.Background(), set)
	if err := s.ScheduleTaskSet(context.Background(), set, newFIFOObserver()); err == nil {
		t.Fatal("duplicate set accepted")
	}
	set.Tasks[0].Operations[0].RDD.Operator.SourcePath = "mutated input"
	if err := s.RegisterWorker("a", 1); err != nil {
		t.Fatal(err)
	}
	a := fifoOffer(t, s, "a", 1)[0]
	if a.Attempt.Task.Operations[0].RDD.Operator.SourcePath != "original" {
		t.Fatal("submission metadata aliased input")
	}
	a.Attempt.Task.Operations[0].RDD.Operator.SourcePath = "mutated assignment"
	running := []plan.TaskAttemptID{a.Attempt.Identity.ID}
	fifoOffer(t, s, "a", 0, running...)
	running[0] = 999
	snapshot, _ := s.Worker("a")
	if snapshot.LastHeartbeat.IsZero() || snapshot.ReportedFreeSlots != 0 || snapshot.RunningAttemptIDs[0] != a.Attempt.Identity.ID {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	snapshot.RunningAttemptIDs[0] = 999
	again, _ := s.Worker("a")
	if again.RunningAttemptIDs[0] != a.Attempt.Identity.ID {
		t.Fatal("snapshot aliases registry")
	}
	records := []any{"original output"}
	report := fifoSuccess(a, 7)
	report.Output.Records = records
	if err := s.ReportSuccess(report); err != nil {
		t.Fatal(err)
	}
	records[0] = "mutated output"
	if err := s.ReportSuccess(fifoSuccess(a, 999)); err != nil {
		t.Fatal(err)
	}
	if err := fifoWait(t, done); err != nil {
		t.Fatal(err)
	}
	if len(observer.successes) != 1 {
		t.Fatal("duplicate result delivered")
	}
	result := <-observer.successes
	if result.Output.Count != 7 || result.Output.Records[0] != "original output" {
		t.Fatalf("result = %#v", result)
	}
	s.mu.Lock()
	path := s.attempts[a.Attempt.Identity.ID].assignment.Attempt.Task.Operations[0].RDD.Operator.SourcePath
	s.mu.Unlock()
	if path != "original" {
		t.Fatal("returned assignment aliases scheduling state")
	}
	// A heartbeat racing a terminal report may still list that known attempt.
	fifoOffer(t, s, "a", 0, a.Attempt.Identity.ID)
}

func TestDAGSchedulerRunsThroughFIFOAssignments(t *testing.T) {
	physical := NewFIFOTaskScheduler()
	defer physical.Close()
	dag := NewDAGScheduler(&recordingFunctionLookup{exists: true}, physical)
	defer dag.Close()
	for _, id := range []plan.WorkerID{"a", "b"} {
		if err := physical.RegisterWorker(id, 2); err != nil {
			t.Fatal(err)
		}
	}
	graph := plan.NewRDDGraph()
	target := addPlannerNode(t, graph, plannerSourceNode(4))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan JobResult, 1)
	errs := make(chan error, 1)
	go func() {
		result, err := dag.Run(ctx, graph, ActionSpec{Kind: ActionCount, TargetRDD: target})
		results <- result
		errs <- err
	}()
	waitFIFOState(t, physical, func() bool { return len(physical.queue) == 1 })
	first := fifoOffer(t, physical, "a", 2)
	second := fifoOffer(t, physical, "b", 2)
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("assignments = %d, %d", len(first), len(second))
	}
	for _, a := range append(second, first...) {
		fifoComplete(t, physical, a)
	}
	if err := fifoWait(t, errs); err != nil {
		t.Fatal(err)
	}
	if result := <-results; result.Count != 4 {
		t.Fatalf("count = %d, want 4", result.Count)
	}
}
