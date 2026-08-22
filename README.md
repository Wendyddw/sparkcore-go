# SparkCore Go

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

The intended package dependency direction is:

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
