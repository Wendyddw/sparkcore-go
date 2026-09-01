package scheduler

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestEventLoopHandlesEventsSerially(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	var mu sync.Mutex
	var handled []plan.JobID
	loop := newEventLoop(func(event schedulerEvent) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		mu.Lock()
		handled = append(handled, event.(jobCanceled).jobID)
		mu.Unlock()
		active.Add(-1)
	})

	for id := plan.JobID(0); id < 4; id++ {
		if err := loop.send(context.Background(), jobCanceled{jobID: id}); err != nil {
			t.Fatalf("send(job %d) error = %v", id, err)
		}
	}
	loop.close()

	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent handlers = %d, want 1", maximum.Load())
	}
	if want := []plan.JobID{0, 1, 2, 3}; !reflect.DeepEqual(handled, want) {
		t.Fatalf("handled jobs = %#v, want %#v", handled, want)
	}
}

func TestEventLoopSendHonorsContextCancellation(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	loop := newEventLoop(func(schedulerEvent) {
		close(handlerStarted)
		<-releaseHandler
	})
	if err := loop.send(context.Background(), jobCanceled{jobID: 1}); err != nil {
		t.Fatalf("first send error = %v", err)
	}
	<-handlerStarted

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.send(ctx, jobCanceled{jobID: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("second send error = %v, want context canceled", err)
	}
	close(releaseHandler)
	loop.close()
}

func TestEventLoopCloseIsIdempotentAndRejectsEvents(t *testing.T) {
	loop := newEventLoop(nil)
	loop.close()
	loop.close()

	err := loop.send(context.Background(), jobCanceled{jobID: 1})
	if !errors.Is(err, errEventLoopStopped) {
		t.Fatalf("send after close error = %v, want stopped", err)
	}
}
