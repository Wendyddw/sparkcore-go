# Mini Spark Scheduler

An educational distributed batch-processing system written in Go and inspired by Apache Spark's RDD execution model.

The project focuses on making this execution flow explicit:

```text
lazy RDD lineage
    -> action submission
    -> DAG and stage planning
    -> one task per stage partition
    -> narrow-operation pipelining
    -> shuffle barriers
    -> worker execution and retry
```

This is a learning project, not a production replacement for Apache Spark.

## Current status

Week 1 is in progress. The first vertical slice will lazily construct and locally execute:

```text
TextFile -> Map -> Filter -> Count
```

It will also explain a `ReduceByKey` pipeline as a shuffle-map stage followed by a result stage, without executing the shuffle yet.

See [WEEK1_IMPLEMENTATION_PLAN.md](WEEK1_IMPLEMENTATION_PLAN.md) for the granular work plan.

## Week 1 scope

- Immutable, lazy RDD lineage.
- Named function registry instead of serialized Go closures.
- Explicit narrow and shuffle dependencies.
- Partitioner metadata propagation.
- Lineage validation and explain output.
- Stage decomposition and partition-level task generation.
- Local iterator execution for narrow pipelines.
- In-process scheduler event loop and local task runner.

## Non-goals

The MVP intentionally excludes Spark SQL, Catalyst, joins, caching, streaming, speculative execution, dynamic executor allocation, sophisticated locality, worker-local shuffle files, disk spilling, and production security or UI work.

## Requirements

- Go 1.26.5 or a compatible newer toolchain.

## Development commands

Run these commands from the repository root:

```bash
go mod tidy
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
```

## Architecture direction

Packages will be introduced as their vertical slice requires them. The intended dependency direction is:

```text
cmd -> api -> scheduler -> executor
        |          |          |
        +--------> plan <-----+
```

The `plan` package contains serializable computation metadata and must not depend on the higher-level API, scheduler, or executor packages.

## Simplifying assumptions

- Functions are registered under stable names shared by the driver and future workers.
- MVP keys are strings and record values are JSON-compatible.
- Local and future worker processes access source data through shared filesystem paths.
- Week 1 executes only narrow pipelines; distributed scheduling and shuffle execution arrive in later slices.

