package coordinator

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func (h *handler) submitJob(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		writeError(w, http.StatusServiceUnavailable, protocol.CodeUnavailable, "job submission is not configured")
		return
	}
	spec, ok := decodeRequest[protocol.SubmitJobRequest](w, r, h.maxRequestBytes)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.jobTimeout)
	defer cancel()

	// Allow the blocking job wait, then retain a bounded response write.
	deadline, _ := ctx.Deadline()
	if !setJobWriteDeadline(w, deadline.Add(h.writeTimeout)) {
		return
	}
	result, err := h.jobs.Submit(ctx, spec)
	if r.Context().Err() != nil {
		return // The submit client has disconnected or canceled its request.
	}
	if !setJobWriteDeadline(w, time.Now().Add(h.writeTimeout)) {
		return
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		writeJobError(w, err)
		return
	}
	if result.Action != spec.Action || result.Validate() != nil {
		writeError(w, http.StatusInternalServerError, protocol.CodeInternal, "invalid job result")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func setJobWriteDeadline(w http.ResponseWriter, deadline time.Time) bool {
	err := http.NewResponseController(w).SetWriteDeadline(deadline)
	// In-memory response writers have no connection deadline to adjust.
	if err == nil || errors.Is(err, http.ErrNotSupported) {
		return true
	}
	writeError(w, http.StatusInternalServerError, protocol.CodeInternal, "cannot set job response deadline")
	return false
}

func writeJobError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, protocol.CodeJobFailed, "job deadline exceeded")
	case errors.Is(err, scheduler.ErrSchedulerClosed), errors.Is(err, scheduler.ErrTaskSchedulerClosed):
		writeError(w, http.StatusServiceUnavailable, protocol.CodeUnavailable, "job scheduling is closed")
	case errors.Is(err, ErrJobFailed):
		writeError(w, http.StatusUnprocessableEntity, protocol.CodeJobFailed, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, protocol.CodeInternal, "job submission failed")
	}
}
