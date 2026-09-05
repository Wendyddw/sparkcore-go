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
bounded local task execution -> partition results -> action result
```

Transformations such as `Map` and `Filter` are lazy: they append serializable metadata to the lineage graph and do not open the source path or run registered functions. `ExplainLineage` and stage planning are also metadata-only operations.

`Count` and `Collect` are eager actions. They validate the reachable lineage and named functions, build stages, generate one logical task per result-stage partition, and run each task's complete narrow pipeline.

## Packages

- `api` provides the driver-facing `Context` and lazy `RDD` operations.
- `plan` owns serializable RDD nodes, dependencies, IDs, validation, and lineage explanation.
- `scheduler` converts lineage into stages and tasks, then coordinates jobs through a single state-owning event loop.
- `executor` contains records, the named-function registry, iterator pipelines, text partition readers, and the bounded local task runner.
- `jobspec` decodes declarative JSON jobs and builds their lazy RDD lineage.
- `cmd/local` runs supported jobs; `cmd/explain` prints lineage and stage plans without executing them.
- `internal/examplefuncs` registers the functions referenced by the included examples.
- `integration` verifies the public API through planning, scheduling, and execution.

The package dependency direction keeps `plan` independent of the API, scheduler, and executor. The scheduler depends on small function-lookup and task-runner interfaces, while the executor implements those runtime boundaries.

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
- Jobs run in one process. There is no coordinator API, remote worker, wire protocol, placement, heartbeat, or retry handling yet.
- The project excludes SQL/Catalyst, joins, caching, streaming, speculative execution, dynamic allocation, advanced locality, disk spilling, production security, and a production UI.

Week 2 introduces coordinator and worker boundaries, transport-friendly requests, worker registration/heartbeats, and retry-oriented task attempts while preserving the Week 1 planning model. Week 3 adds the shared shuffle store and executable one-shuffle `ReduceByKey` path.
