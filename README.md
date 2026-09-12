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
- `coordinator` exposes worker registration, heartbeat assignments, and terminal task reports through an injectable HTTP server.
- `cmd/local` runs supported jobs; `cmd/explain` prints lineage and stage plans without executing them.
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

`coordinator.NewServer(taskScheduler, config)` returns a standard `*http.Server` with an injected scheduling dependency. The caller starts it with `Serve` or `ListenAndServe` and drains active requests with `Shutdown(ctx)`. Scheduler shutdown remains the caller's responsibility.

The worker HTTP API supports:

- `POST /v1/workers/register`: register a worker and its capacity. Repeating the same registration returns `200`; changing its capacity returns `409` without changing reservations. Optional `base_url` is validated but unused by heartbeat polling.
- `POST /v1/workers/heartbeat`: validate reported capacity and running attempts, then return FIFO assignments up to available slots. No work returns `{"assignments":[]}`.
- `POST /v1/tasks/success`: deliver partition output to the scheduler, which validates the assignment, releases its reservation once, and queues an observer callback.
- `POST /v1/tasks/failure`: report a terminal error. The scheduler fails the task set without retry and keeps sibling reservations until those attempts report completion.

Successful report handling returns `200` with `{"acknowledged":true}`, including duplicate and obsolete reports. Acknowledgment confirms safe handling, not a state change. Late reports after task-set failure or cancellation release outstanding reservations without completion callbacks. Count stays `int64`; Collect records remain `json.RawMessage` elements in scheduler output to preserve numeric precision.

Responses use `application/json`. Errors carry the protocol's stable code: invalid input is `400`, unknown workers/attempts/endpoints `404`, unsupported methods `405` with `Allow: POST`, registration or report-identity conflicts `409`, oversized requests `413`, closed scheduling `503`, and unexpected failures `500`.

Defaults are `127.0.0.1:8080`, a 1 MiB request limit, 5-second header reads, 10-second reads/writes, and 60-second idle connections. `Config` can override these values. Tests use `httptest` and the real FIFO scheduler to verify registration, assignment metadata, report callbacks, duplicate/late reports, concurrent offers/reports, reservations, and draining shutdown. Job submission, worker runtime, and distributed commands follow in later sessions.

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

## Development checks

```bash
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
```

## Assumptions and limitations

- Functions are registered under stable string IDs shared by the driver and future workers; arbitrary Go closures are not serialized.
- Records use JSON-compatible values, and keys are strings.
- Source paths refer to a shared filesystem. Local execution reads them directly, and future workers are assumed to see the same paths.
- Only narrow pipelines execute in Week 1. Shuffle storage, shuffle-map execution, reduce fetches, and barriers are not implemented yet.
- Jobs still run in one process. Coordinator registration, heartbeat, and task report endpoints are implemented; job submission, worker runtime, and distributed commands remain pending. Heartbeat expiry and retry handling are deferred to Week 3.
- The project excludes SQL/Catalyst, joins, caching, streaming, speculative execution, dynamic allocation, advanced locality, disk spilling, production security, and a production UI.

Week 2 introduces coordinator and worker boundaries, transport-friendly requests, worker registration/heartbeats, and retry-oriented task attempts while preserving the Week 1 planning model. Week 3 adds the shared shuffle store and executable one-shuffle `ReduceByKey` path.
