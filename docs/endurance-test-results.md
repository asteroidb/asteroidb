# Endurance & Large-Scale Test Methodology

This document describes the methodology and usage of AsteroidDB's large-scale
node and long-running endurance tests.  These tests verify that the system
behaves correctly under sustained load, detects memory leaks, and confirms
convergence after fault injection.

## Test Overview

| Test Script | Purpose | Default Duration | Nodes |
|---|---|---|---|
| `scripts/test-large-scale.sh` | Sustained write load, memory tracking, node join/leave | 5 min | 7 |
| `scripts/test-endurance.sh` | Mixed workload with periodic partition/heal cycles | 30 min | 7 |

Both tests use `docker-compose.scale.yml`, which defines a 7-node full-mesh
cluster with dedicated configs in `configs/scale/`.

## Infrastructure

### Cluster Topology

```
docker-compose.scale.yml
  node-1 (port 3001) ─┐
  node-2 (port 3002) ─┤  Authority nodes (quorum = 3)
  node-3 (port 3003) ─┘
  node-4 (port 3004) ─┐
  node-5 (port 3005) ─┤  Non-authority data nodes
  node-6 (port 3006) ─┤
  node-7 (port 3007) ─┘
```

All nodes run in `Both` mode (store + subscribe) with full-mesh peer
connectivity.  Authority quorum is formed by nodes 1-3.

### Config Structure

- `configs/scale/node-{1..7}.json` — 7-node full-mesh peer configs
- `configs/node-{1..3}.json` — Original 3-node configs (unchanged)

## Large-Scale Test (`test-large-scale.sh`)

### What It Tests

1. **Cluster bootstrap** — Verifies all 7 nodes start and become healthy
2. **Sustained write load** — Configurable number of concurrent writers
   distribute writes across all nodes for the specified duration
3. **Memory leak detection** — Compares baseline memory (post-startup) with
   final memory after load; flags >3x growth as a potential leak
4. **Convergence** — After load stops, writes a key to node-1 and verifies
   it propagates to all 7 nodes within 60 seconds
5. **Node join/leave** — Stops node-7 mid-load, continues writing, then
   restarts node-7 and verifies it catches up via delta sync
6. **Time-series metrics** — Samples memory and CPU usage per node at
   configurable intervals, outputting CSV for offline analysis

### Usage

```bash
# Default: 5 minutes, 10 concurrent writers, 10s sample interval
./scripts/test-large-scale.sh

# Extended: 30 minutes with 20 writers
./scripts/test-large-scale.sh --duration 1800 --concurrency 20

# Quick smoke test: 60 seconds
./scripts/test-large-scale.sh --duration 60 --concurrency 3 --sample-interval 5
```

### Output Files

| File | Format | Contents |
|---|---|---|
| `target/large-scale-metrics.csv` | CSV | Time-series: timestamp, elapsed_s, node, memory_mb, cpu_pct |
| `target/large-scale-results.json` | JSON | Summary: writes, errors, throughput, convergence, leak detection |

### Metrics CSV Schema

```csv
timestamp_s,elapsed_s,node,memory_mb,cpu_pct
1710000000,0,node-1,45.2,2.3
1710000000,0,node-2,44.8,1.9
...
```

## Endurance Test (`test-endurance.sh`)

### What It Tests

1. **Mixed workload** — Concurrent writers alternate between register-set
   operations and counter increments (2:1 ratio), distributing across all nodes
2. **Periodic partition/heal** — Every N seconds (default 120), a non-authority
   node is isolated via iptables for M seconds (default 15), then healed
3. **Convergence tracking** — After each heal cycle, writes a convergence-check
   key and verifies all nodes see it within 5 seconds
4. **Memory trend** — Continuous metric sampling tracks memory growth over the
   full test duration
5. **Final convergence** — After the test completes, writes a final key and
   verifies all 7 nodes converge

### Fault Injection Strategy

Only non-authority nodes (4-7) are partitioned, cycling through them in
round-robin order.  This ensures the authority quorum (nodes 1-3) is never
broken, so certified writes remain possible throughout.

Partition is implemented via iptables DROP rules on the target container,
blocking both inbound and outbound traffic to all peers.

### Usage

```bash
# Default: 30 minutes
./scripts/test-endurance.sh

# Quick: 5 minutes with faster partitions
./scripts/test-endurance.sh --duration 300 --partition-interval 60 --partition-duration 10

# Long soak: 24 hours
./scripts/test-endurance.sh --duration 86400 --partition-interval 300 --sample-interval 60
```

### Output Files

| File | Format | Contents |
|---|---|---|
| `target/endurance-metrics.csv` | CSV | Memory and CPU per node over time |
| `target/endurance-convergence.csv` | CSV | Per-check convergence results |
| `target/endurance-partitions.csv` | CSV | Partition start/heal events |
| `target/endurance-results.json` | JSON | Summary of the full test run |

## Interpreting Results

### Memory Leak Detection

Both tests flag a potential memory leak if any node's memory exceeds 3x its
baseline measurement.  This is a conservative threshold — genuine leaks in CRDT
GC, AckFrontier GC, or compaction would manifest as unbounded growth well
beyond 3x.

For more precise analysis, plot the `memory_mb` column from the metrics CSV
over time.  A healthy system should show memory plateau after initial growth.

### Convergence Failures

Transient convergence failures during partition/heal cycles are expected (the
partitioned node cannot receive updates).  The key metric is **final
convergence** — after all partitions are healed and load stops, all nodes must
agree.

A non-zero `final_convergence_failures` value indicates a bug in delta sync,
full sync fallback, or CRDT merge.

### Throughput

Throughput is reported as approximate ops/sec.  This is total successful writes
divided by wall-clock duration.  For more precise measurement, use
`scripts/bench-multinode.sh` with controlled parameters.

## Prerequisites

- Docker and docker compose
- curl, python3, flock
- Sufficient resources: 7 containers require ~2 GB RAM and 4+ CPU cores
- Ports 3001-3007 must be free

## CI Integration

These tests are not part of the standard CI gate (`cargo test`) since they
require Docker and run for extended durations.  Run them manually or in a
dedicated CI job:

```yaml
# Example GitHub Actions step
- name: Large-scale test (smoke)
  run: ./scripts/test-large-scale.sh --duration 120 --concurrency 5
```
