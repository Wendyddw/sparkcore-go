# SparkCore Go

A Spark-style execution core in Go for exploring lazy computation graphs, stage planning, and distributed task scheduling. It supports narrow pipelines and one-shuffle `ReduceByKey` jobs with `Count` and `Collect` actions. Shuffle execution uses a shared filesystem directory configured on executors.

## Architecture

- **API and planning** (`api`, `plan`, `jobspec`): build lazy RDD lineage from Go code or JSON job specifications, then validate and split it into stages.
- **Scheduling** (`scheduler`): the DAG scheduler waits for every parent map output before dispatching reducers; the FIFO scheduler assigns the oldest task set's pending partitions in ascending order, within worker capacity.
- **Coordinator** (`coordinator`, `protocol`): exposes HTTP/JSON endpoints for job submission, worker registration, heartbeats, and task reports.
- **Execution** (`worker`, `executor`, `shuffle`): workers poll for tasks and execute narrow pipelines or shuffle writes/reads through `LocalRunner`. Shuffle bytes stay in shared storage; HTTP carries metadata. Local mode uses the same DAG scheduler with a local scheduling adapter.

```text
Submitter -> Coordinator -> DAG scheduler -> FIFO scheduler
                                                |
                                   worker heartbeat pulls tasks
                                                |
                                                v
Submitter <- merged result <- task reports <- Workers
```

Results complete after all partitions succeed: `Count` sums partition counts, and `Collect` merges records in partition order. Attempt identities protect accepted results and slot accounting from stale or duplicate reports.

Distributed FIFO scheduling retries execution failures up to three total attempts per task, preserving successful partitions and FIFO priority. Configure the limit with `scheduler.WithMaxTaskAttempts(n)`; one disables retries and nonpositive values panic. Known validation/data errors fail immediately; custom functions can mark these with `scheduler.PermanentFailure(err)`. Local execution does not retry ordinary task failures, and HTTP requests are not automatically retried.

Missing or corrupt accepted shuffle input triggers DAG recovery in both local and distributed execution: discard partial results, rerun the full map stage, then rerun reducers with the new outputs. The default allows two executions per stage (one restart). Pass `scheduler.WithMaxStageAttempts(n)` to the DAG scheduler or job service to configure this separate budget; one disables recovery and nonpositive values panic. Old attempts cannot change replacement results or overwrite their files.

The coordinator checks worker liveness every second and marks workers lost after 10 seconds without a valid heartbeat, starting from registration. Unfinished assignments use the same retry budget; accepted shuffle outputs remain available in shared storage. Lost worker IDs cannot register or receive work again. Embedded coordinators must run and join `coordinator.WorkerMonitor`; its timeout and interval are configurable through `WorkerMonitorConfig`.

## Run

Requires Go 1.26.5 or a compatible newer toolchain. Run commands from the repository root.

Run the example locally:

```bash
go run ./cmd/local --job examples/count.json
```

For distributed execution, start a coordinator and two workers in separate terminals:

```bash
go run ./cmd/coordinator --listen 127.0.0.1:8080
go run ./cmd/worker --coordinator http://127.0.0.1:8080 --id worker-1 --slots 2
go run ./cmd/worker --coordinator http://127.0.0.1:8080 --id worker-2 --slots 2
```

Submit a job from another terminal:

```bash
go run ./cmd/submit --coordinator http://127.0.0.1:8080 --job examples/count.json
```

Both examples print `5`. Collect jobs print a JSON array. Results go to stdout; lifecycle logs and command errors go to stderr. Use `--help` for flags and Ctrl-C to stop processes. Stop workers before the coordinator to allow their final reports to be acknowledged.

Run the shuffle example locally:

```bash
go run ./cmd/local --job examples/reduce_by_key.json --shuffle-root /tmp/sparkcore-demo-shuffle
```

For distributed shuffle, add `--shuffle-root /tmp/sparkcore-demo-shuffle` to **both worker commands**, then submit `examples/reduce_by_key.json`. Every worker must use the same absolute directory on shared storage. The coordinator needs no storage flag: it sends metadata and a fresh run namespace with assignments. The example uses four map partitions and two reduce partitions and collects `alpha=2`, `beta=2`, `gamma=1`; changing its action to `count` returns `3`.

The coordinator exposes `--max-task-attempts` (default 3), `--max-stage-attempts` (2), `--worker-timeout` (10s), and `--worker-check-interval` (1s). Attempt limits include the initial execution; 1 disables the corresponding retry/recovery. All values must be positive, and the check interval must not exceed the timeout. Local mode exposes the stage limit only. Keep worker heartbeat intervals comfortably below the timeout.

Structured logs include `task_attempt_failed`, `task_requeued`, `task_failed` with `budget_exhausted`, `worker_lost`, `shuffle_output_accepted`, and `shuffle_recovery_started`. Attempt identities connect assignments, failures and accepted outputs.

Published files are retained across job completion and process restarts. After stopping every process using this demo directory, remove it explicitly:

```bash
rm -rf /tmp/sparkcore-demo-shuffle
```

Inspect a shuffle plan without executing it:

```bash
go run ./cmd/explain --job examples/reduce_by_key.json
```

`examples/reduce_by_key_explain.json` intentionally references a missing source to verify that explanation does not read input.

## Constraints

- Workers must have access to source files. Relative paths resolve from each worker's working directory.
- The coordinator and workers must register matching function IDs and implementations; arbitrary Go closures are not serialized. The commands register the included example functions.
- Records must be JSON-compatible, and keys are strings.
- `--shuffle-root` is optional for narrow jobs and required for shuffle execution. Embedded callers configure `executor.WithShuffleStore` or `worker.RuntimeConfig.ShuffleStore`. Store handles close after execution stops; files remain until explicit cleanup.
- Multiple shuffles and disk spilling are not implemented.
- Heartbeat expiry is suspicion: an old worker may still execute. Attempt isolation and report fencing protect accepted results, but cannot undo external side effects. Restart a lost worker with a new ID.
- Canceling a submission stops pending scheduling; already assigned tasks may finish and report. Workers continue polling until stopped.

## Development

```bash
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
```

The [narrow integration tests](integration/distributed_narrow_test.go) and [shuffle integration tests](integration/dag_shuffle_test.go) exercise HTTP submission through two workers, including stage readiness, Count/Collect results, worker capacity, duplicate reports, and cleanup.

[Retry tests](integration/task_retry_test.go) cover bounded task failures; [worker-loss tests](integration/worker_loss_test.go) verify that a survivor finishes using accepted shuffle output from a lost worker. [Stage-recovery tests](integration/stage_recovery_test.go) damage published buckets and verify local/HTTP recovery and its attempt limit.
