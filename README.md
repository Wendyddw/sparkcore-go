# SparkCore Go

SparkCore Go is a from-scratch implementation of a Spark-style execution core in Go. The project is a deep dive into the systems behind distributed computation: DAG construction, lazy execution, stage decomposition, shuffle boundaries, task scheduling, failure and retry semantics, and distributed execution abstractions.

These mechanisms extend beyond batch processing. The same concepts underpin distributed data pipelines, model training systems, evaluation infrastructure, and other platforms that must turn a logical computation into reliable partition-level work across machines.

## Systems focus

The project follows the complete path from a user-defined computation to distributed execution:

```text
DAG construction
    -> lazy execution
    -> stage decomposition
    -> shuffle boundaries
    -> task scheduling
    -> failure and retry semantics
    -> distributed execution abstractions
```

Each layer is kept explicit so its responsibilities and boundaries can be inspected independently. The current implementation starts with local execution while preserving the planning and task interfaces needed for remote workers, retries, and shuffle coordination in later phases.

## Execution workflow

```text
TextFile / transformations
        |
        v
driver-owned RDD lineage metadata
        |
        | Count or Collect
        v
validation -> stage planning -> task generation
        |
        v
DAG scheduler event loop
        |
        v
LocalTaskScheduler -> bounded LocalRunner.RunTask
        |
        v
partition reports -> DAG scheduler -> action result
```

Transformations such as `Map` and `Filter` are lazy: they append serializable metadata to the lineage graph and do not open the source path or run registered functions. `ExplainLineage` and stage planning are also metadata-only operations.

`Count` and `Collect` are eager actions. They validate the reachable lineage and named functions, build stages, generate one logical task per result-stage partition, and run each task's complete narrow pipeline.

## Packages

- `api` provides the driver-facing `Context` and lazy `RDD` operations.
- `plan` owns serializable RDD nodes, dependencies, IDs, validation, and lineage explanation.
- `scheduler` converts lineage into stages and tasks, then coordinates jobs through a single state-owning event loop.
- `executor` contains records, the named-function registry, iterator pipelines, text partition readers, the local task-set scheduling adapter, and the bounded local task runner.
- `jobspec` decodes declarative JSON jobs and builds their lazy RDD lineage.
- `protocol` defines the `/v1` HTTP/JSON messages, bounded strict decoding, and message validation.
- `coordinator` exposes worker lifecycle HTTP endpoints and a job service that builds lineage and runs actions through the DAG scheduler.
- `worker` provides the coordinator HTTP client and the polling runtime that executes assigned tasks through `LocalRunner`.
- `cmd/local` runs supported jobs; `cmd/explain` prints lineage and stage plans without executing them.
- `cmd/coordinator` hosts job and worker APIs; `cmd/worker` runs an independently configured worker process; `cmd/submit` sends job files and prints final results.
- `internal/examplefuncs` registers the functions referenced by the included examples.
- `integration` verifies the public API through planning, scheduling, and execution.

The package dependency direction keeps `plan` independent of the API, scheduler, and executor. The DAG scheduler depends on small function-lookup and task-set scheduler interfaces, while the executor implements those local runtime boundaries. `LocalTaskScheduler` assigns an attempt ID to each task and calls `LocalRunner.RunTask`; the DAG event loop owns stage completion and Count/Collect merging. `FIFOTaskScheduler` implements the same interface for placement through worker registration, resource offers, and terminal reports. Coordinator handlers adapt HTTP requests to scheduler calls; the scheduler owns placement state.

## FIFO task placement

`FIFOTaskScheduler` assigns the oldest task set's pending partitions in ascending order. `RegisterWorker` records a stable worker ID and positive slot capacity; `OfferResources` records free slots and running attempt IDs and returns assignments with unique attempt IDs. `ReportSuccess` and `ReportFailure` accept a terminal result once and release its reservation.

Assignments in transit remain reserved even when a heartbeat does not list them. Failure or cancellation stops further assignment for the task set, but sibling reservations remain until workers report that those attempts have ended. Duplicate and obsolete results cannot overwrite accepted output. Observer callbacks run outside the placement lock, and `Close` waits for submissions and callbacks to exit.

The registry retains heartbeat timestamps for observation only. Worker expiry and retries are deferred; task-set and attempt history are retained for the scheduler instance's lifetime to recognize duplicate reports. The local command continues to use `LocalTaskScheduler`.

## Protocol contracts

The `/v1` messages cover worker registration, heartbeat assignments, terminal task reports, job submission/results, and structured errors. `protocol.DecodeAndValidate[T](reader, maxBytes)` enforces a whole-body byte limit, exactly one object, known and unique field names, required fields, and semantic validation. Explicit zero IDs remain valid; missing or null numeric fields are rejected. Fields tagged `omitempty` may be absent. Result records remain raw JSON values to preserve their structure and integer precision.

Heartbeat decoding validates the reported fields; handlers additionally call `ValidateCapacity(totalSlots)` with registered capacity. Worker lookup, attempt ownership, reservations, and duplicate result acceptance stay in the scheduler. The HTTP server sets I/O deadlines; the decoder bounds bytes but does not own the connection.

## Coordinator HTTP service

`coordinator.NewServer(taskScheduler, jobSubmitter, config)` returns a standard `*http.Server` with injected worker-scheduling and job-submission dependencies. Pass a `JobService` backed by the same FIFO scheduler; a nil submitter leaves worker endpoints available and returns `503` for job submissions. The caller starts it with `Serve` or `ListenAndServe` and drains active requests with `Shutdown(ctx)`. Scheduler shutdown remains the caller's responsibility.

The HTTP API supports:

- `POST /v1/workers/register`: register a worker and its capacity. Repeating the same registration returns `200`; changing its capacity returns `409` without changing reservations. Optional `base_url` is validated but unused by heartbeat polling.
- `POST /v1/workers/heartbeat`: validate reported capacity and running attempts, then return FIFO assignments up to available slots. No work returns `{"assignments":[]}`.
- `POST /v1/tasks/success`: deliver partition output to the scheduler, which validates the assignment, releases its reservation once, and queues an observer callback.
- `POST /v1/tasks/failure`: report a terminal error. The scheduler fails the task set without retry and keeps sibling reservations until those attempts report completion.
- `POST /v1/jobs`: strictly decode the existing job specification, wait for distributed Count/Collect completion, and return `JobResultResponse` as JSON with status `200`.

Successful report handling returns `200` with `{"acknowledged":true}`, including duplicate and obsolete reports. Acknowledgment confirms safe handling, not a state change. Late reports after task-set failure or cancellation release outstanding reservations without completion callbacks. Count stays `int64`; Collect records remain `json.RawMessage` elements in scheduler output to preserve numeric precision.

Responses use `application/json`. Errors carry the protocol's stable code: invalid input is `400`, unknown workers/attempts/endpoints `404`, unsupported methods `405` with `Allow: POST`, registration or report-identity conflicts `409`, oversized requests `413`, closed scheduling `503`, and unexpected failures `500`. Job planning/execution failures use `422` with `job_failed`; the job deadline uses `504` with `job_failed`. Internal error details are omitted.

Defaults are `127.0.0.1:8080`, a 1 MiB request limit, 5-second header reads, 10-second reads/writes, and 60-second idle connections. `Config` can override these values. Tests use `httptest` and the real FIFO scheduler to verify registration, assignment metadata, report callbacks, duplicate/late reports, concurrent offers/reports, reservations, and draining shutdown. Job submissions have a separate five-minute `Config.JobTimeout`. Their response write deadline includes the job wait, then returns to the configured write budget. ResponseWriter middleware must expose `Unwrap` or deadline methods to preserve this behavior. Coordinator, worker and submit commands are available.

## Coordinator job service

`coordinator.NewJobService(registry, taskScheduler)` starts an owned DAG scheduler. Pass the same FIFO scheduler to this service and the worker HTTP server. `Submit(ctx, spec)` builds a fresh lazy graph through the public API and blocks for Count/Collect completion. Concurrent jobs have separate graphs; the coordinator does not open source files or execute partition pipelines.

Accepted worker reports flow through FIFO observer callbacks into the DAG event loop. Results complete after all partitions succeed: Count is summed and Collect is merged in partition order with JSON integer precision preserved. Invalid plans, unknown functions and shuffle jobs fail before assignment. `Close()` cancels jobs and closes the DAG scheduler; the caller closes the shared FIFO scheduler separately.

Canceling a submission context stops pending scheduling. Already assigned workers may finish; their late reports release reservations without changing the job result. The HTTP handler passes the request context into `Submit` with a finite job deadline, so a disconnected submit client or expired deadline cancels scheduling. Workers keep polling after a job completes or fails. `ErrJobFailed` distinguishes planning/action failures from internal errors while preserving wrapped cancellation and scheduler-closure causes.

## Worker HTTP client

`worker.NewClient(coordinatorURL, worker.ClientConfig{})` provides context-aware `RegisterWorker`, `Heartbeat`, `ReportSuccess`, and `ReportFailure` calls using the protocol DTOs. It validates outgoing messages and bounded JSON responses, with a default 10-second request timeout and 1 MiB response limit. The client supports concurrent calls, closes response bodies, and does not follow redirects or perform application-level retries.

Registration replies must match the requested identity and capacity. Heartbeat assignments must target the requesting worker, fit the offered slots, and avoid already-running attempt IDs. `worker.HTTPError` preserves coordinator HTTP status, error code, and message for `errors.As`; `ErrInvalidResponse` identifies malformed or inconsistent replies. Context and size errors remain available through `errors.Is`.

The client is tested against the coordinator and FIFO scheduler, including duplicate terminal reports and precise JSON record payloads. It starts no background loops; the runtime drives the worker lifecycle.

## Worker runtime

`worker.NewRuntime(client, worker.RuntimeConfig{...})` creates an independent function registry and `LocalRunner`. Supply a worker ID, positive slot count, and a `RegisterFunctions` hook for the named functions used by jobs. Sources default to the text reader and remain unopened until a task executes.

`Run(ctx)` registers once, polls immediately, and then sends periodic heartbeats containing free slots and sorted active attempt IDs. The default polling interval is 100ms and each coordinator call has a 10-second timeout. One loop owns attempt tracking; task goroutines execute and report concurrently. A slot stays occupied until its terminal report is acknowledged. The runtime validates a complete assignment batch before dispatch and rejects excess work, foreign assignments and reused attempts.

Tasks execute through `LocalRunner.RunTask`. Count and Collect outputs are converted to protocol messages, preserving empty arrays and JSON integer precision. Task or output-encoding errors become failure reports without retries; after an acknowledged task failure, the worker continues polling for other work.

Canceling Run stops polling, cancels execution, and waits for every task goroutine. Terminal reports get a separate bounded context to notify the coordinator during shutdown. Sources, functions and injected clients must cooperate with cancellation. Communication/protocol errors stop the runtime; failed reports may leave reservations until future worker-loss recovery. Each Runtime supports one startup.

Integration tests run two worker instances through the real HTTP service and FIFO scheduler with separate function registries. Four narrow partitions produce Count `5` and the expected Collect records. End-to-end tests submit JSON through `POST /v1/jobs`, execute real text sources through two workers, return the final result, and run more jobs after a worker reports a source failure. Subprocess tests also verify the coordinator and worker command entry points, independent function registration, and interrupt handling.

## Dependencies and stages

A narrow dependency maps each child partition directly to its parent partition. Consecutive narrow transformations are pipelined into one stage and one task processes the full pipeline for a single partition.

A shuffle dependency requires data to be repartitioned. `ReduceByKey` therefore introduces a boundary between a `ShuffleMapStage` and a dependent `ResultStage`. Week 1 can validate, plan, and explain that two-stage DAG, but it deliberately rejects shuffle execution before dispatching tasks.

## Run the Week 1 demos

Requires Go 1.26.5 or a compatible newer toolchain. From the repository root, run:

```bash
go run ./cmd/local --job examples/count.json
```

Expected output:

```text
5
```

The example runs `TextFile -> Map -> Filter -> Count` over four local partitions.

To inspect a shuffle plan without reading its intentionally missing input file, run:

```bash
go run ./cmd/explain --job examples/reduce_by_key.json
```

The output contains the RDD lineage followed by a four-task `shuffle_map` stage and a dependent two-task `result` stage.

## Distributed commands

Run these in separate terminals from the repository root:

```bash
go run ./cmd/coordinator --listen 127.0.0.1:8080
go run ./cmd/worker --coordinator http://127.0.0.1:8080 --id worker-1 --slots 2
go run ./cmd/worker --coordinator http://127.0.0.1:8080 --id worker-2 --slots 2
```

Submit an example from another terminal:

```bash
go run ./cmd/submit --coordinator http://127.0.0.1:8080 --job examples/count.json
```

The command prints `5`. Collect jobs print a JSON array with raw record precision preserved. The coordinator and worker commands register the example functions independently. All workers must see the source files at the paths in the job specification; relative paths resolve from each worker's working directory. Deploy matching function IDs and implementations in the coordinator and workers.

Coordinator flags include `--listen` (default `127.0.0.1:8080`), `--job-timeout` (5m), and `--shutdown-timeout` (10s). Worker flags include `--coordinator`, required `--id`, `--slots` (2), `--heartbeat-interval` (100ms), and `--request-timeout` (10s). Polling frequency is configured by workers. Use `--help` for usage; unknown flags, extra arguments and nonpositive durations/capacities are rejected.

The submit command accepts `--coordinator`, required `--job`, `--request-timeout` (6m), and `--max-response-bytes` (1 MiB). Job files are strictly validated and limited to 1 MiB; source paths are sent unchanged for workers to resolve. Its timeout covers the POST and complete response body. It rejects redirects, does not retry submissions, and validates the result before printing. Increase the response limit for larger Collect results. The default client timeout exceeds the coordinator's default five-minute job budget.

Ctrl-C or SIGTERM initiates daemon shutdown. The coordinator closes active jobs, drains HTTP within its shutdown timeout, and closes the FIFO scheduler; blocked submissions receive an unavailable response when the connection remains writable. Workers cancel their runtime, wait for task goroutines and allow bounded terminal reports. Stop workers before the coordinator when those reports need to be acknowledged. Operational errors exit with status 1; normal signal shutdown exits with status 0.

Submission failure, timeout or signal cancellation exits with status 1 and prints an error on stderr; stdout remains empty until a complete valid result is ready. Successful submission exits with status 0 and prints only the result. Help goes to stderr and exits successfully. Canceling the submitter cancels its HTTP request and coordinator scheduling; already assigned workers may still finish and report.

Process startup/shutdown diagnostics use JSON `slog` events on stderr. Daemon stdout stays empty. Detailed job, stage and task logging remains a later Session 7 batch.

## Development checks

```bash
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
```

## Assumptions and limitations

- Functions are registered under stable string IDs shared by the driver and workers; arbitrary Go closures are not serialized.
- Records use JSON-compatible values, and keys are strings.
- Source paths refer to a shared filesystem. Local execution and workers must see the same paths.
- Only narrow pipelines execute in Week 1. Shuffle storage, shuffle-map execution, reduce fetches, and barriers are not implemented yet.
- Coordinator and worker processes can execute narrow jobs submitted over HTTP. Detailed scheduler lifecycle logs remain pending. Heartbeat expiry and retry handling are deferred to Week 3.
- The project excludes SQL/Catalyst, joins, caching, streaming, speculative execution, dynamic allocation, advanced locality, disk spilling, production security, and a production UI.

Week 2 introduces coordinator and worker boundaries, transport-friendly requests, worker registration/heartbeats, and retry-oriented task attempts while preserving the Week 1 planning model. Week 3 adds the shared shuffle store and executable one-shuffle `ReduceByKey` path.
