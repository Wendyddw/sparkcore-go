package coordinator

import (
	"net/http"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func (h *handler) taskSuccess(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest[protocol.TaskSuccessRequest](w, r, h.maxRequestBytes)
	if !ok {
		return
	}
	output := scheduler.TaskOutput{Count: request.Output.Count}
	if request.Output.Records != nil {
		output.Records = make([]any, len(request.Output.Records))
		for i, record := range request.Output.Records {
			// Keep raw JSON through scheduler merging to preserve numeric precision.
			output.Records[i] = record
		}
	}
	err := h.tasks.ReportSuccess(scheduler.TaskAttemptSuccess{
		JobID: request.JobID, StageID: request.StageID, Attempt: request.Attempt,
		PartitionID: request.PartitionID, WorkerID: request.WorkerID, Output: output,
	})
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.TaskReportResponse{Acknowledged: true})
}

func (h *handler) taskFailure(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest[protocol.TaskFailureRequest](w, r, h.maxRequestBytes)
	if !ok {
		return
	}
	err := h.tasks.ReportFailure(scheduler.TaskAttemptFailure{
		JobID: request.JobID, StageID: request.StageID, Attempt: request.Attempt,
		PartitionID: request.PartitionID, WorkerID: request.WorkerID, Error: request.Error,
	})
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.TaskReportResponse{Acknowledged: true})
}
