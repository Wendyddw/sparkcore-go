package scheduler

import (
	"context"
	"errors"
	"sync"
)

var errEventLoopStopped = errors.New("scheduler event loop is stopped")

// eventHandler mutates scheduler-owned state for one event.
// The event loop never invokes it concurrently.
type eventHandler func(schedulerEvent)

// eventLoop serializes every scheduler state transition on one goroutine.
type eventLoop struct {
	events    chan schedulerEvent
	stop      chan struct{}
	done      chan struct{}
	handler   eventHandler
	closeOnce sync.Once
}

func newEventLoop(handler eventHandler) *eventLoop {
	loop := &eventLoop{
		events:  make(chan schedulerEvent),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		handler: handler,
	}
	go loop.run()
	return loop
}

// send queues an event or returns when the caller or loop stops.
func (l *eventLoop) send(ctx context.Context, event schedulerEvent) error {
	if l == nil {
		return errEventLoopStopped
	}
	select {
	case l.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return errEventLoopStopped
	}
}

// close stops the loop after the current handler returns.
func (l *eventLoop) close() {
	if l == nil {
		return
	}
	l.closeOnce.Do(func() { close(l.stop) })
	<-l.done
}

func (l *eventLoop) run() {
	defer close(l.done)
	for {
		select {
		case event := <-l.events:
			if l.handler != nil {
				l.handler(event)
			}
		case <-l.stop:
			return
		}
	}
}
