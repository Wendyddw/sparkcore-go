# SparkCore Go

A Spark-style execution core in Go for exploring lazy computation graphs, stage planning, and distributed task scheduling. It supports local and distributed narrow pipelines with `Count` and `Collect` actions. Shuffle boundaries can be planned and explained; shuffle execution is not implemented yet.

## Architecture

- **API and planning** (`api`, `plan`, `jobspec`): build lazy RDD lineage from Go code or JSON job specifications, then validate and split it into stages.
- **Scheduling** (`scheduler`): the DAG scheduler owns job and stage completion; the FIFO scheduler assigns the oldest task set's pending partitions in ascending order, within worker capacity.
- **Coordinator** (`coordinator`, `protocol`): exposes HTTP/JSON endpoints for job submission, worker registration, heartbeats, and task reports.
- **Execution** (`worker`, `executor`): workers poll for assignments and execute each partition's narrow pipeline through `LocalRunner`. Local mode uses the same DAG scheduler with a local scheduling adapter.

```text
Submitter -> Coordinator -> DAG scheduler -> FIFO scheduler
                                                |
                                   worker heartbeat pulls tasks
                                                |
                                                v
Submitter <- merged result <- task reports <- Workers
```

Results complete after all partitions succeed: `Count` sums partition counts, and `Collect` merges records in partition order. Attempt identities protect accepted results and slot accounting from stale or duplicate reports.

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

Both examples print `5`. Collect jobs print a JSON array. Results go to stdout; daemon lifecycle logs and command errors go to stderr. Use `--help` for flags and Ctrl-C to stop processes. Stop workers before the coordinator to allow their final reports to be acknowledged.

Inspect a shuffle plan without executing it:

```bash
go run ./cmd/explain --job examples/reduce_by_key.json
```

## Constraints

- Workers must have access to source files. Relative paths resolve from each worker's working directory.
- The coordinator and workers must register matching function IDs and implementations; arbitrary Go closures are not serialized. The commands register the included example functions.
- Records must be JSON-compatible, and keys are strings.
- Shuffle execution, retries, and worker-loss recovery are not implemented.
- Canceling a submission stops pending scheduling; already assigned tasks may finish and report. Workers continue polling until stopped.

## Development

```bash
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
```

The [distributed integration tests](integration/distributed_narrow_test.go) exercise HTTP submission through two workers, checking Count/Collect results, worker capacity, unique attempts, stale and duplicate reports, and cleanup.
