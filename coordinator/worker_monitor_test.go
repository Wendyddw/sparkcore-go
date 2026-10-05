package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

type expiryFunc func(time.Time, time.Duration) ([]plan.WorkerID, error)

func (f expiryFunc) ExpireWorkers(now time.Time, timeout time.Duration) ([]plan.WorkerID, error) {
	return f(now, timeout)
}

func TestWorkerMonitorConfiguration(t *testing.T) {
	expire := expiryFunc(func(time.Time, time.Duration) ([]plan.WorkerID, error) { return nil, nil })
	for _, config := range []WorkerMonitorConfig{{WorkerTimeout: -1}, {CheckInterval: -1}, {WorkerTimeout: time.Second, CheckInterval: 2 * time.Second}} {
		if _, err := NewWorkerMonitor(expire, config); err == nil {
			t.Fatal("invalid monitor configuration accepted")
		}
	}
	if _, err := NewWorkerMonitor(nil, WorkerMonitorConfig{}); err == nil {
		t.Fatal("nil expirer accepted")
	}
	m, err := NewWorkerMonitor(expire, WorkerMonitorConfig{})
	if err != nil || m.config.WorkerTimeout != 10*time.Second || m.config.CheckInterval != time.Second {
		t.Fatalf("defaults = %+v, %v", m, err)
	}
	if err := m.Run(nil); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestWorkerMonitorTicksAndJoinsInFlightExpiry(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ticks := make(chan time.Time, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := NewWorkerMonitor(expiryFunc(func(got time.Time, timeout time.Duration) ([]plan.WorkerID, error) {
		if got != now || timeout != 10*time.Second {
			t.Errorf("expiry arguments = %v, %v", got, timeout)
		}
		close(entered)
		<-release
		return nil, nil
	}), WorkerMonitorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.run(ctx, ticks) }()
	ticks <- now
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("expiry did not start")
	}
	cancel()
	ticks <- now.Add(time.Second)
	select {
	case <-done:
		close(release)
		t.Fatal("monitor returned before active expiry completed")
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop")
	}
}

func TestWorkerMonitorRunsWithoutHTTPAndPropagatesErrors(t *testing.T) {
	want := errors.New("expiry failed")
	m, err := NewWorkerMonitor(expiryFunc(func(time.Time, time.Duration) ([]plan.WorkerID, error) { return nil, want }),
		WorkerMonitorConfig{CheckInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Run(ctx); !errors.Is(err, want) {
		t.Fatalf("monitor error = %v", err)
	}
	cancel()
	if err := m.Run(ctx); err != nil {
		t.Fatal(err)
	}
}
