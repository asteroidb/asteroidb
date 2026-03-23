# High-Latency Scenario Test Results

## Overview

This document describes the methodology and expected results format for the
high-latency scenario tests (`scripts/test-high-latency.sh`), which measure
AsteroidDB cluster behavior under escalating network latency from 100ms to
multi-second RTT.

These tests are relevant to AsteroidDB's target deployment environments where
nodes may be geographically distributed (cross-region DC, satellite links) and
experience significant propagation delays.

## Test Methodology

### Cluster Setup

- 3-node AsteroidDB cluster via Docker Compose (`docker-compose.yml`)
- All nodes have `CAP_NET_ADMIN` for `tc netem` injection
- Network delay is applied to **node-2** and **node-3** using `tc qdisc add dev eth0 root netem delay <ms>`
- node-1 remains delay-free and serves as the write origin

### Latency Levels

| Level | One-Way Delay | Effective RTT | Real-World Analog |
|-------|--------------|---------------|-------------------|
| Baseline | 0ms | ~0ms | Same-rack / loopback |
| 100ms | 100ms | ~200ms | Cross-continent (US-EU) |
| 500ms | 500ms | ~1s | Satellite ground relay |
| 1000ms | 1000ms | ~2s | GEO satellite link |
| 3000ms | 3000ms | ~6s | Deep-space / degraded link |

### Measurements

At each latency level, three metrics are captured:

#### (a) Delta Sync Convergence Time

- Write 5 counter increments to node-1
- Poll node-2 and node-3 until the counter value equals 5
- Record wall-clock time from write completion to convergence on each node
- Timeout: configurable (default 120s, doubled for >= 1s delay)

This measures the end-to-end delta sync propagation pipeline including:
- node-1 outbound delta batch interval
- Network transit (affected by injected delay)
- node-2/3 inbound processing and merge

#### (b) Write/Read Throughput

- **Write throughput**: Sequential `POST /api/eventual/write` (counter_inc) to node-1
- **Read throughput**: Sequential `GET /api/eventual/<key>` from node-2 (through delayed path)
- Measures: ops/sec, latency percentiles (p50, p95, p99)
- Default: 50 operations per level

Write throughput to node-1 should remain relatively stable since writes are
local-accept (eventual consistency). Read throughput from node-2 is impacted
by the return-path delay on the HTTP response.

#### (c) Certified Write Latency

- `POST /api/certified/write` to node-1 with `on_timeout=pending`
- Requires majority quorum (2/3 nodes) to acknowledge
- At high latency, certification round-trips increase proportionally
- Records: p50/p95/p99 latency, success count, timeout count
- Reduced to 5 ops at 3s delay to keep test duration bounded

## Output Format

### CSV (`target/high-latency-results.csv`)

```csv
latency_ms,convergence_node2_ms,convergence_node3_ms,write_throughput_ops_s,write_p50_us,write_p95_us,write_p99_us,read_throughput_ops_s,read_p50_us,read_p95_us,read_p99_us,cert_p50_us,cert_p95_us,cert_p99_us,cert_success,cert_timeouts
0,150,180,245.50,3200,5100,8400,220.10,3800,6200,9100,45000,62000,78000,10,0
100,1200,1350,230.00,3500,5800,9200,45.20,21000,24000,28000,220000,280000,310000,10,0
500,3500,3800,225.00,3800,6200,10100,9.80,102000,108000,115000,1050000,1200000,1350000,10,0
1000,8200,8800,210.50,4100,6800,11500,4.90,204000,210000,218000,2100000,2400000,2600000,10,0
3000,-1,-1,195.00,4500,7500,13000,1.60,612000,620000,635000,6200000,6800000,7200000,3,2
```

Column definitions:
- `latency_ms`: Injected one-way delay (0 = baseline)
- `convergence_node{2,3}_ms`: Time for delta sync to propagate (-1 = timeout)
- `write_throughput_ops_s`: Eventual writes per second to node-1
- `write_p{50,95,99}_us`: Write latency percentiles in microseconds
- `read_throughput_ops_s`: Eventual reads per second from node-2
- `read_p{50,95,99}_us`: Read latency percentiles in microseconds
- `cert_p{50,95,99}_us`: Certified write latency percentiles in microseconds
- `cert_success`: Number of certified writes that completed
- `cert_timeouts`: Number of certified writes that timed out

### JSON (`target/high-latency-results.json`)

```json
{
  "test": "high-latency-scenarios",
  "timestamp": "2026-03-10T12:00:00Z",
  "config": {
    "write_ops_per_level": 50,
    "convergence_timeout_s": 120,
    "latency_levels_ms": [100, 500, 1000, 3000],
    "skip_certified": false,
    "nodes": 3
  },
  "results": [
    {
      "latency_ms": 0,
      "convergence": { "node_2_ms": 150, "node_3_ms": 180 },
      "write": { "throughput_ops_s": 245.50, "p50_us": 3200, "p95_us": 5100, "p99_us": 8400 },
      "read": { "throughput_ops_s": 220.10, "p50_us": 3800, "p95_us": 6200, "p99_us": 9100 },
      "certified": { "p50_us": 45000, "p95_us": 62000, "p99_us": 78000, "success": 10, "timeouts": 0 }
    }
  ]
}
```

## Expected Behavior

### Convergence Time

Convergence time scales roughly linearly with RTT:
- At 100ms delay: convergence ~ 1-2s (dominated by sync interval + 1 RTT)
- At 500ms delay: convergence ~ 3-5s
- At 1s delay: convergence ~ 6-10s
- At 3s delay: convergence may timeout depending on sync backoff settings

### Write Throughput

Eventual writes to node-1 should remain **relatively stable** across all
latency levels because writes are locally accepted. Minor degradation may
occur due to background sync pressure.

### Read Throughput

Read throughput from a delayed node degrades proportionally to RTT because
each HTTP response must traverse the delayed network path:
- Baseline: ~200+ ops/s
- 100ms: ~40-50 ops/s
- 500ms: ~8-10 ops/s
- 1s: ~4-5 ops/s
- 3s: ~1-2 ops/s

### Certified Write Latency

Certified writes require majority quorum round-trips. With 2 of 3 nodes
delayed, the fastest quorum path includes at least one delayed node:
- Certified p50 ~ 2 * delay_ms (one round-trip to nearest authority)
- At 3s delay, most certified writes are expected to timeout

## Usage

```bash
# Full test (all latency levels, including certified writes)
./scripts/test-high-latency.sh

# Quick test with fewer ops
./scripts/test-high-latency.sh --write-ops 20

# Skip certified writes (faster)
./scripts/test-high-latency.sh --skip-certified

# Extended convergence timeout for very high latency
./scripts/test-high-latency.sh --convergence-timeout 300
```

## Interpreting Results

The test always exits 0 because convergence timeouts at extreme latency are
**expected behavior**, not bugs. The purpose is to produce quantitative data
showing how AsteroidDB degrades gracefully under high latency.

Key things to verify:
1. **No data loss**: Eventual writes succeed locally regardless of network delay
2. **Graceful degradation**: Throughput and convergence scale predictably with RTT
3. **Recovery**: After delay removal, the cluster returns to baseline performance
