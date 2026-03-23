# Satellite Constellation Scenario Tests

## Overview

This document describes the methodology and expected behaviors for the AsteroidDB satellite constellation scenario tests. These tests simulate space-like network conditions to validate that AsteroidDB's CRDT-based eventual consistency and delta sync mechanisms operate correctly under extreme latency, packet loss, and intermittent connectivity conditions.

## Motivation

AsteroidDB is designed to serve workloads spanning ground data centers to satellite constellations. The satellite use case represents the most demanding deployment scenario:

- **High latency**: Geostationary satellites have 500-600ms RTT to ground stations
- **Intermittent connectivity**: LEO satellites orbit every ~90 minutes with limited visible windows per ground station
- **Mixed workloads**: Telemetry data (eventual consistency) and control commands (certified writes) coexist
- **Data accumulation**: Writes accumulate during blackout periods and must converge during contact windows

These scenario tests validate that AsteroidDB handles all of these conditions without data loss.

## Test Infrastructure

### Cluster Setup

Tests run on a 3-node Docker cluster with `tc netem` for network condition injection and `iptables` for link blackouts:

| Node | Role | Container |
|------|------|-----------|
| node-1 | Ground station | asteroidb-node-1 |
| node-2 | Satellite node | asteroidb-node-2 |
| node-3 | Relay / secondary ground | asteroidb-node-3 |

All containers have `NET_ADMIN` capability for traffic control.

### Network Injection Tools

- **`tc netem`**: Injects delay, jitter, and packet loss at the kernel level
- **`iptables`**: Simulates complete link blackouts by dropping all packets

### Measurement Methodology

For each scenario, the following metrics are collected:

1. **Delta sync convergence time (ms)**: Time from last write to full convergence across nodes, measured by polling the target node until the expected counter value appears
2. **Write throughput (ops/s)**: Number of eventual writes completed per second under the given network conditions
3. **Read throughput (ops/s)**: Number of reads completed per second under the given network conditions
4. **Data integrity**: All nodes must converge to the same CRDT state (no data loss, no divergence)

Results are saved as JSON to `target/satellite-results/<scenario>.json`.

## Scenarios

### Scenario 1: LEO (Low Earth Orbit)

**Network conditions:**
- RTT: 20-40ms (injected as 30ms +/- 10ms normally distributed)
- Packet loss: 3%
- Connectivity: Intermittent (visible window + blackout cycle)

**Test phases:**
1. **Visible window**: Satellite writes telemetry data during contact. Delta sync propagates to ground station.
2. **Blackout**: Satellite goes behind Earth (iptables DROP). Ground station writes control commands.
3. **Restore**: Contact restored. Verify all accumulated writes converge bidirectionally.

**Expected behavior:**
- Convergence during visible window: < 10 seconds
- Post-blackout convergence: < 15 seconds (delta sync backlog drain)
- Zero data loss across blackout/restore cycles
- Write throughput: Minimally impacted by 3% loss (CRDT retries handle drops)

### Scenario 2: MEO (Medium Earth Orbit)

**Network conditions:**
- RTT: 100-150ms (injected as 125ms +/- 25ms normally distributed)
- Packet loss: 0.5%
- Connectivity: Stable (no blackouts)

**Test phases:**
1. Write data from satellite node
2. Verify convergence to ground station through the higher-latency link
3. Measure throughput under sustained latency

**Expected behavior:**
- Convergence: < 20 seconds (higher RTT means slower sync rounds)
- Write throughput: Reduced proportionally to RTT increase
- Zero data loss (stable link, low loss)
- Delta sync batching should amortize per-op RTT cost

### Scenario 3: GEO (Geostationary Orbit)

**Network conditions:**
- RTT: 500-600ms (injected as 550ms +/- 50ms normally distributed)
- Packet loss: 0.1%
- Connectivity: Stable (GEO satellites maintain fixed position)

**Test phases:**
1. Write data from satellite node through high-latency link
2. Wait for propagation through 500ms+ RTT path
3. Verify convergence and integrity

**Expected behavior:**
- Convergence: < 60 seconds (each sync round incurs 500ms+ RTT)
- Write throughput: Significantly reduced (each HTTP round-trip takes >1s)
- Zero data loss (stable link, minimal loss)
- This scenario stress-tests timeout handling and backoff mechanisms

### Scenario 4: ISL (Inter-Satellite Link)

**Network conditions:**
- RTT: 50-80ms (injected as 65ms +/- 20ms normally distributed on both node-2 and node-3)
- Packet loss: 0.2% per link
- Both satellite nodes affected (simulating direct laser link)

**Test phases:**
1. Bidirectional writes: satellite-1 (node-2) and satellite-2 (node-3) write concurrently
2. Verify cross-satellite convergence (each satellite sees the other's data)
3. Verify ground station (node-1) receives all data from both paths

**Expected behavior:**
- Cross-satellite convergence: < 15 seconds
- Bidirectional CRDT merge: No conflicts (PN-Counter is commutative)
- Ground station receives union of all writes
- Jitter may cause out-of-order delivery, but CRDT semantics handle this

### Scenario 5: Ground-to-LEO Handover

**Network conditions:**
- Baseline: LEO conditions (30ms +/- 10ms delay, 2% loss)
- Periodic blackouts: 15s visible / 10s dark, repeated 3 times
- Compressed timeline (real LEO pass is ~90 min cycle, 15-20 min visible)

**Test phases:**
1. **Pass 1**: Satellite writes telemetry, syncs during visible window, goes dark
2. **Blackout 1**: Ground station sends control commands while satellite is dark
3. **Pass 2**: Contact restored, backlog drains, new writes accumulate
4. **Blackout 2**: Another dark period
5. **Pass 3**: Final contact window, verify all accumulated data converges

**Expected behavior:**
- Data accumulates correctly across multiple passes
- Each pass drains the delta sync backlog from the previous blackout
- Control commands sent during blackout eventually reach the satellite
- Final convergence across all 3 nodes with zero data loss
- GC remains stable across multiple accumulation/drain cycles

## Running the Tests

```bash
# Run all scenarios
./scripts/test-satellite-scenario.sh

# Run a specific scenario
./scripts/test-satellite-scenario.sh --scenario leo

# Quick mode (reduced write counts)
./scripts/test-satellite-scenario.sh --quick
```

## Result Format

Each scenario produces a JSON file in `target/satellite-results/`:

```json
{
  "timestamp": "2026-03-10T12:00:00Z",
  "scenario": "leo",
  "status": "pass",
  "convergence_time_ms": 5432,
  "write_throughput_ops_per_sec": 45.2,
  "read_throughput_ops_per_sec": 120.8,
  "data_integrity_ok": true
}
```

## Key Design Decisions

1. **Compressed timelines**: Real satellite passes take 90+ minutes. Tests compress to seconds/minutes while preserving the essential pattern (visible -> dark -> visible).

2. **iptables for blackouts**: Using `iptables -j DROP` instead of `tc netem loss 100%` for blackouts because iptables provides true bidirectional packet dropping (both ingress and egress), more accurately simulating complete loss of signal.

3. **CRDT counters for validation**: PN-Counter is used as the test data type because its commutative, associative merge semantics make convergence verification deterministic: if every node shows the same counter value equaling the total writes, data integrity is confirmed.

4. **Separate keys per scenario**: Each scenario uses unique keys (with PID in the name) to avoid interference between test runs.

## Relationship to Production

These tests validate the core properties required for satellite deployment:

| Property | How Tested |
|----------|------------|
| Eventual consistency under partition | LEO blackout + handover scenarios |
| High-latency convergence | GEO scenario (500ms+ RTT) |
| Bidirectional replication | ISL scenario (cross-satellite sync) |
| Multi-pass data accumulation | Handover scenario (3 passes) |
| Delta sync backoff under loss | LEO/MEO loss injection |
| CRDT merge correctness | All scenarios verify data integrity |
