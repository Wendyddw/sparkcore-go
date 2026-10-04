package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/Wendyddw/sparkcore-go/shuffle"
)

type FailureKind string

const (
	FailureExecution    FailureKind = "execution"
	FailurePermanent    FailureKind = "permanent"
	FailureShuffleInput FailureKind = "shuffle_input"
	FailureCanceled     FailureKind = "canceled"
)

// ValidateFailure checks worker-reported categories and their required metadata.
func ValidateFailure(kind FailureKind, input *shuffle.InputReference) error {
	switch kind {
	case FailureShuffleInput:
		if input == nil {
			return fmt.Errorf("shuffle_input failure requires input identity")
		}
		return input.Validate()
	case FailureExecution, FailurePermanent, FailureCanceled:
		if input != nil {
			return fmt.Errorf("only shuffle_input failure may contain input identity")
		}
		return nil
	default:
		return fmt.Errorf("unknown failure kind %q", kind)
	}
}

// ClassifyFailure preserves structured input loss; unclassified errors are execution failures.
func ClassifyFailure(ctx context.Context, err error) (FailureKind, *shuffle.InputReference) {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return FailureCanceled, nil
	}
	var input *shuffle.InputError
	if errors.As(err, &input) {
		return FailureShuffleInput, &shuffle.InputReference{Attempt: input.Attempt, PartitionID: input.PartitionID}
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) || errors.Is(err, shuffle.ErrInvalidRecord) || errors.Is(err, shuffle.ErrRecordTooLarge) {
		return FailurePermanent, nil
	}
	return FailureExecution, nil
}
