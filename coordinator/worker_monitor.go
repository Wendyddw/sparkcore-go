package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// WorkerExpirer owns expiry decisions and assignment recovery under its own lock.
type WorkerExpirer interface {
	ExpireWorkers(time.Time, time.Duration) ([]plan.WorkerID, error)
}

// WorkerMonitorConfig uses a 10s timeout and 1s check interval when fields are zero.
type WorkerMonitorConfig struct {
	WorkerTimeout time.Duration
	CheckInterval time.Duration
}

// WorkerMonitor drives expiry independently of incoming HTTP requests.
// The caller runs it once, cancels its context and joins Run before closing tasks.
type WorkerMonitor struct {
	tasks  WorkerExpirer
	config WorkerMonitorConfig
}

// NewWorkerMonitor validates settings without starting background work.
func NewWorkerMonitor(tasks WorkerExpirer, config WorkerMonitorConfig) (*WorkerMonitor, error) {
	if tasks == nil {
		return nil, fmt.Errorf("worker expirer is nil")
	}
	if config.WorkerTimeout < 0 || config.CheckInterval < 0 {
		return nil, fmt.Errorf("worker monitor durations must not be negative")
	}
	if config.WorkerTimeout == 0 {
		config.WorkerTimeout = 10 * time.Second
	}
	if config.CheckInterval == 0 {
		config.CheckInterval = time.Second
	}
	if config.CheckInterval > config.WorkerTimeout {
		return nil, fmt.Errorf("worker check interval must not exceed timeout")
	}
	return &WorkerMonitor{tasks: tasks, config: config}, nil
}

// Run blocks until canceled or an expiry operation fails. Cancellation returns nil.
func (m *WorkerMonitor) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("worker monitor context is nil")
	}
	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()
	return m.run(ctx, ticker.C)
}

func (m *WorkerMonitor) run(ctx context.Context, ticks <-chan time.Time) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticks:
			if ctx.Err() != nil {
				return nil
			}
			if _, err := m.tasks.ExpireWorkers(now, m.config.WorkerTimeout); err != nil {
				return fmt.Errorf("expire workers: %w", err)
			}
		}
	}
}
