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

## Criterion Benchmarks (Micro-benchmarks)

In addition to the high-level benchmarks above, AsteroidDB includes Criterion
micro-benchmarks covering CRDT operations, store operations, certification
paths, and cryptographic signatures.

### Running Criterion Benchmarks Locally

```bash
# Run all Criterion benchmarks
cargo bench

# Run a specific benchmark suite
cargo bench --bench crdt_bench
cargo bench --bench store_bench
cargo bench --bench certified_bench
cargo bench --bench signature_bench

# Run the sync comparison benchmark (custom harness)
cargo bench --bench sync_benchmark

# Save a named baseline for later comparison
cargo bench -- --save-baseline my-baseline

# Compare against a saved baseline
cargo bench -- --baseline my-baseline
```

### Available Benchmark Suites

| Suite | File | What it measures |
|-------|------|------------------|
| `crdt_bench` | `benches/crdt_bench.rs` | PnCounter, OrSet, OrMap, LwwRegister operations and merges |
| `store_bench` | `benches/store_bench.rs` | Store put/get, entries_since, snapshot save/load |
| `certified_bench` | `benches/certified_bench.rs` | Certified write, process_certifications, proof verification |
| `signature_bench` | `benches/signature_bench.rs` | BLS vs Ed25519 keygen/sign/verify, aggregate operations, DualModeCertificate |
| `sync_benchmark` | `benches/sync_benchmark.rs` | Full sync vs delta sync payload size comparison |

### Comparing Two Runs Manually

Use `scripts/bench-compare.sh` to compare two sets of Criterion results:

```bash
# 1. Run benchmarks with a baseline name
cargo bench -- --save-baseline before

# 2. Make your changes, then run again
cargo bench -- --save-baseline after

# 3. Compare the two
bash scripts/bench-compare.sh \
  target/criterion   \  # baseline (uses 'before' data)
  target/criterion      # current  (uses latest run data)
```

The script flags any benchmark that regressed by more than 10% (configurable
via `BENCH_REGRESSION_THRESHOLD` environment variable).

## CI Benchmark Pipeline

The project runs automated benchmark regression detection via GitHub Actions.

### Schedule

- **Weekly**: Every Monday at 04:00 UTC (cron schedule)
- **Manual**: Can be triggered via `workflow_dispatch` in the Actions tab

### How It Works

1. The workflow (`.github/workflows/benchmark.yml`) runs all Criterion
   benchmark suites on a fresh `ubuntu-latest` runner.
2. Results are saved as GitHub Actions artifacts with 90-day retention.
3. On subsequent runs, the workflow downloads the previous run's artifact and
   compares using `scripts/bench-compare.sh`.
4. A summary table is posted to the GitHub Actions step summary showing
   baseline vs current timings and percentage change.
5. Any benchmark that regressed by more than 10% is flagged with a warning
   annotation on the workflow run.

### Reading CI Results

1. Go to **Actions** > **Benchmark Regression Check** in the GitHub UI.
2. Open the latest run and check the **step summary** for the comparison table.
3. Download the `benchmark-results` artifact for raw Criterion data.
4. Download the `benchmark-comparison` artifact for the full comparison report.

### Triggering a Manual Run

```bash
# Via GitHub CLI
gh workflow run benchmark.yml
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
