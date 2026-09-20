package shuffle

import (
	"context"
	"errors"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

var (
	ErrClosed       = errors.New("shuffle handle is closed")
	ErrOutputExists = errors.New("shuffle attempt output already exists")
	ErrStoreBusy    = errors.New("shuffle store has active handles")
	ErrShuffleInput = errors.New("shuffle input unavailable or corrupt")
)

// Store separates publication from reading accepted output. Callers own scheduler
// acceptance; publishing alone never makes an attempt eligible for reduce work.
type Store interface {
	Begin(ctx context.Context, attempt AttemptIdentity, numMapPartitions, numReducePartitions int) (AttemptWriter, error)
	OpenBucket(ctx context.Context, output MapOutput, partition plan.PartitionID) (BucketReader, error)
}

// AttemptWriter hashes records into buckets. Publish closes the attempt; Abort
// discards private files. Cancellation and write/publish errors abort automatically.
type AttemptWriter interface {
	Write(Record) error
	Publish() (MapOutput, error)
	Abort() error
}

// BucketReader verifies input before returning io.EOF. Closing early releases
// resources but does not certify integrity. Cancellation closes it automatically.
type BucketReader interface {
	Next() (Record, error)
	Close() error
}

// InputError identifies a failed accepted input and preserves its underlying cause.
type InputError struct {
	Attempt     AttemptIdentity
	PartitionID plan.PartitionID
	Err         error
}

func (e *InputError) Error() string {
	return fmt.Sprintf("shuffle %d map %d attempt %d bucket %d: %v",
		e.Attempt.ShuffleID, e.Attempt.MapPartitionID, e.Attempt.TaskAttemptID, e.PartitionID, e.Err)
}

func (e *InputError) Unwrap() error     { return e.Err }
func (e *InputError) Is(err error) bool { return err == ErrShuffleInput }
