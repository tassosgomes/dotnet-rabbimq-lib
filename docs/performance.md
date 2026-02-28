# Performance Methodology

This repository tracks two different performance dimensions:

## 1. Microbenchmarks

Project: `benchmarks/Rmq.CloudEvents.Benchmarks`

Purpose:

- isolate CPU cost and allocations;
- expose serialization and CloudEvents overhead;
- detect regressions in hot paths that do not require a live broker.

Current benchmark focus:

- `CloudEventWrapper.Wrap`
- `CloudEventWrapper.Unwrap`
- `SystemTextJsonMessageSerializer.Serialize`
- `SystemTextJsonMessageSerializer.Deserialize`

## 2. RabbitMQ-backed performance scenarios

Project: `tests/Rmq.CloudEvents.PerformanceTests`

Purpose:

- measure end-to-end throughput and latency with a real RabbitMQ broker;
- capture memory usage during publish and consume load;
- expose the current operational profile of the library.

Current scenarios:

- direct publish throughput;
- concurrent direct publish throughput;
- publish/consume roundtrip latency;
- topic publish throughput.

## Metrics

Every scenario exports the following fields:

- `scenario`
- `messageCount`
- `payloadBytes`
- `parallelism`
- `durationMilliseconds`
- `throughputMessagesPerSecond`
- `averageLatencyMilliseconds`
- `p95LatencyMilliseconds`
- `managedMemoryBytes`
- `workingSetBytes`
- `peakWorkingSetBytes`
- `privateMemoryBytes`
- `recordedAtUtc`

## GitHub Actions

Workflow: `.github/workflows/performance.yml`

Artifacts published by the workflow:

- BenchmarkDotNet raw artifacts
- per-scenario JSON result files
- `performance-summary.md`

These numbers should be used as a comparative baseline, not as absolute capacity guarantees, because GitHub-hosted runners are noisy by nature.

## Current baseline

Latest local run captured during implementation of the performance harness:

| Scenario | Messages | Payload (B) | Parallelism | Duration (ms) | Throughput (msg/s) | Avg Latency (ms) | P95 Latency (ms) | Working Set (MB) | Peak Working Set (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `direct-publish-throughput` | 1000 | 256 | 1 | 19457.84 | 51.39 | 0.00 | 0.00 | 138.23 | 138.23 |
| `direct-publish-throughput-concurrent` | 1000 | 256 | 8 | 5063.98 | 197.47 | 0.00 | 0.00 | 128.55 | 131.52 |
| `publish-consume-roundtrip` | 200 | 128 | 1 | 3731.94 | 53.59 | 17.90 | 30.89 | 137.10 | 140.55 |
| `topic-publish-throughput` | 500 | 192 | 1 | 9861.92 | 50.70 | 0.00 | 0.00 | 137.85 | 137.85 |

Environment notes:

- local execution with Docker-backed RabbitMQ via Testcontainers;
- numbers are suitable as a regression baseline, not as a universal capacity statement;
- the concurrent publish scenario exists specifically to make future throughput improvements measurable.
