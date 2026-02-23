# AsteroidDB Benchmark Guide

## Overview

AsteroidDB benchmarks measure three key performance indicators that characterize
the system's behaviour under normal operation and failure recovery:

| # | Metric | What it measures |
|---|--------|------------------|
| 1 | **Eventual write latency** | Time to accept a single `eventual_counter_inc` operation locally |
| 2 | **Certified confirmation time** | End-to-end time from `certified_write` to `Certified` status via majority frontier advancement |
| 3 | **Recovery convergence time** | Time for 3 divergent nodes to reach identical state via CRDT merge after partition recovery |

## Running the Benchmark

### Prerequisites

- Rust toolchain (edition 2024)
- Repository cloned and dependencies resolved (`cargo build`)

### Execution

```bash
# Run the benchmark (human-readable progress on stderr, JSON on stdout)
cargo run --example benchmark

# Save JSON results to a file
cargo run --example benchmark > results.json

# Release mode for more representative numbers
cargo run --release --example benchmark > results.json
```

### Output

The benchmark prints progress and CSV summary to **stderr** and a JSON array of
results to **stdout**.

JSON schema per entry:

```json
{
  "name": "eventual_write_latency",
  "iterations": 1000,
  "mean_us": 1.23,
  "p50_us": 1.10,
  "p95_us": 2.50,
  "p99_us": 4.00,
  "min_us": 0.80,
  "max_us": 15.00
}
```

## Metric Details

### 1. Eventual Write Latency

- **Operation**: `EventualApi::eventual_counter_inc` on distinct keys
- **Iterations**: 1000
- **Measures**: Wall-clock time of a single local CRDT write (no network)
- **Relevance**: FR-002/FR-004 -- baseline cost of the eventual consistency path

### 2. Certified Confirmation Time

- **Operation**: `CertifiedApi::certified_write` followed by 2-of-3 authority
  frontier updates and `process_certifications`
- **Iterations**: 100
- **Measures**: Full certification round-trip (write + frontier sync + status check)
- **Relevance**: FR-003/FR-004 -- time to achieve majority consensus confirmation

### 3. Recovery Convergence Time

- **Operation**: 3-node partition scenario with divergent PN-Counter state,
  then full CRDT merge propagation
- **Iterations**: 100
- **Measures**: Wall-clock time from start of merge propagation to all 3 nodes
  holding identical state
- **Relevance**: FR-002/NFR -- demonstrates CRDT convergence guarantee after
  network partition

## Result Recording Template

Copy the table below and fill in the measured values. Include the commit hash
and hardware description for reproducibility.

```
## Benchmark Results

**Date**: YYYY-MM-DD
**Commit**: <hash>
**Hardware**: <CPU / RAM / OS>
**Build mode**: release | debug

| Metric                      | Iterations | Mean (us) | P50 (us) | P95 (us) | P99 (us) | Min (us) | Max (us) |
|-----------------------------|------------|-----------|----------|----------|----------|----------|----------|
| eventual_write_latency      |            |           |          |          |          |          |          |
| certified_confirmation_time |            |           |          |          |          |          |          |
| recovery_convergence_time   |            |           |          |          |          |          |          |

### Notes

- (describe any anomalies, environment specifics, etc.)
```

## Reproducing Results

1. Check out the target commit:
   ```bash
   git checkout <commit-hash>
   ```

2. Build in release mode:
   ```bash
   cargo build --release --example benchmark
   ```

3. Run the benchmark and save results:
   ```bash
   cargo run --release --example benchmark > results.json 2> benchmark.log
   ```

4. Extract CSV from the log:
   ```bash
   grep -A4 "Results (CSV)" benchmark.log
   ```

5. Compare with previous runs using the JSON output:
   ```bash
   # Example: compare mean latencies with jq
   jq '.[].mean_us' results.json
   ```

## Programmatic Access

The metrics module (`src/ops/metrics.rs`) exposes:

- `BenchmarkResult` -- serializable struct with all statistics
- `collect_latencies(name, &[Duration]) -> BenchmarkResult` -- compute stats from raw durations
- `to_csv_row(&BenchmarkResult) -> String` -- CSV formatting
- `csv_header() -> &str` -- matching CSV header

These can be used in integration tests or custom benchmarks:

```rust
use std::time::{Duration, Instant};
use asteroidb_poc::ops::metrics::{collect_latencies, BenchmarkResult};

let mut durations = Vec::new();
for _ in 0..100 {
    let start = Instant::now();
    // ... operation to measure ...
    durations.push(start.elapsed());
}
let result: BenchmarkResult = collect_latencies("my_benchmark", &durations);
println!("{}", serde_json::to_string_pretty(&result).unwrap());
```
