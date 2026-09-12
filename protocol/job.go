package protocol

import (
	"encoding/json"

	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

const SubmitJobPath = "/v1/jobs"

// SubmitJobRequest reuses the existing job format for POST /v1/jobs without
// another envelope or schema. Bounded strict HTTP decoding is added separately.
type SubmitJobRequest = jobspec.Spec

// JobResultResponse is the completed result of a blocking job submission.
// Action selects the result: Count uses Count and nil Records; Collect uses
// Records (an empty slice for no records) and zero Count. Job failures are
// returned as ErrorResponse with an unsuccessful HTTP status.
type JobResultResponse struct {
	Action  scheduler.ActionKind `json:"action"`
	Records []json.RawMessage    `json:"records"`
	Count   int64                `json:"count"`
}
