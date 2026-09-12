package coordinator

import (
	"net/http"

	"github.com/Wendyddw/sparkcore-go/protocol"
)

func (h *handler) registerWorker(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest[protocol.RegisterWorkerRequest](w, r, h.maxRequestBytes)
	if !ok {
		return
	}
	if err := h.tasks.RegisterWorker(request.WorkerID, request.TotalSlots); err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.RegisterWorkerResponse{
		WorkerID: request.WorkerID, TotalSlots: request.TotalSlots,
	})
}

func (h *handler) heartbeat(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest[protocol.HeartbeatRequest](w, r, h.maxRequestBytes)
	if !ok {
		return
	}
	worker, err := h.tasks.Worker(request.WorkerID)
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	if err := request.ValidateCapacity(worker.TotalSlots); err != nil {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, err.Error())
		return
	}
	assignments, err := h.tasks.OfferResources(request.WorkerID, request.FreeSlots, request.RunningAttemptIDs)
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	response := protocol.HeartbeatResponse{Assignments: make([]protocol.TaskAssignment, 0, len(assignments))}
	for _, assignment := range assignments {
		response.Assignments = append(response.Assignments, protocol.TaskAssignment{
			JobID:    assignment.JobID,
			StageID:  assignment.StageID,
			WorkerID: assignment.Attempt.WorkerID,
			Attempt:  assignment.Attempt.Identity,
			Task:     assignment.Attempt.Task,
		})
	}
	writeJSON(w, http.StatusOK, response)
}
