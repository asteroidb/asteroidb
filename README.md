# AsteroidDB

A distributed key-value store that unifies eventual and certified consistency
in a single cluster -- designed for environments ranging from multi-region
data centers to high-latency satellite constellations.

## Key Features

- **Dual consistency model** -- choose per-operation between availability-first
  *eventual* writes (CRDT-based) and authority-confirmed *certified* writes.
- **CRDT-native storage** -- PN-Counter, OR-Set, OR-Map, and LWW-Register with
  automatic conflict-free merge after network partitions.
- **BLS threshold signatures** -- majority certificates backed by BLS12-381
  aggregate signatures (with Ed25519 fallback).
- **Tag-based placement** -- no fixed topology hierarchy; replica placement is
  controlled by arbitrary node tags, required/forbidden constraints, and
  latency-aware ranking.
- **SLO monitoring** -- built-in error-budget tracking for certification
  latency, sync failure rate, and frontier skew.
- **Control plane** -- system namespace stores placement policies and authority
  definitions, updated via quorum consensus.

## Architecture

```
                         +-----------+
                         |  Client   |
                         +-----+-----+
                               |  HTTP API
              +----------------+----------------+
              |                |                |
        +-----v-----+   +-----v------+   +-----v--------+
        | Data Plane |   | Authority  |   | Control      |
        |            |   | Plane      |   | Plane        |
        | CRDT Store |   | Majority   |   | System NS    |
        | Delta Sync |   | Consensus  |   | Tag Policies |
        | Compaction |   | ack_frontier|  | Keyset Mgmt  |
        +-----+------+  | Certificate|   +-----+--------+
              |          +-----+------+         |
              +----------------+---------+------+
                               |
                    +----------v-----------+
                    |     Node Layer       |
                    | store / subscribe /  |
                    | both                 |
                    | Tag-based Placement  |
                    +----------------------+
```

**Data Plane** -- Handles CRDT reads/writes, anti-entropy delta sync between
peers, and log compaction. Writes are locally accepted and propagate
asynchronously.

**Authority Plane** -- A per-key-range group of authority nodes. When a
majority acknowledges an update (tracked by HLC-based `ack_frontier`), a
`majority_certificate` is issued. Clients can request certified reads with
cryptographic proof.

**Control Plane** -- Manages placement policies and authority definitions in
a `system namespace`. Mutations require quorum consensus among control-plane
authority nodes.

## Quick Start

### Prerequisites

- Rust toolchain (edition 2024, 1.85+)
- Docker & Docker Compose (for multi-node cluster)

### Build

```bash
cargo build --release
```

### Run a single node

```bash
cargo run
# Listening on 127.0.0.1:3000
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ASTEROIDB_BIND_ADDR` | `127.0.0.1:3000` | HTTP listen address |
| `ASTEROIDB_NODE_ID` | `node-1` | Unique node identifier |
| `ASTEROIDB_ADVERTISE_ADDR` | same as bind | Address advertised to peers |
| `ASTEROIDB_INTERNAL_TOKEN` | *(none)* | Bearer token for inter-node auth |
| `ASTEROIDB_DATA_DIR` | `./data` | Persistence directory |
| `ASTEROIDB_CONFIG` | *(none)* | Path to JSON config file |
| `ASTEROIDB_BLS_SEED` | *(none)* | Hex-encoded 32-byte BLS key seed |

### Run a 3-node cluster with Docker Compose

```bash
# Start
docker compose up -d --build

# Check health
scripts/cluster-status.sh

# Stop
docker compose down
```

Nodes are exposed on `localhost:3001`, `localhost:3002`, `localhost:3003`.

### Run the interactive demo

```bash
scripts/demo.sh
```

## API Examples

### Eventual write (LWW Register)

```bash
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"register_set","key":"sensor-1","value":"23.5"}'
```

### Eventual read

```bash
curl -s http://localhost:3001/api/eventual/sensor-1 | jq .
# {"key":"sensor-1","value":{"type":"register","value":"23.5"}}
```

### CRDT counter operations

```bash
# Increment a counter
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"counter_inc","key":"page-views"}'

# Read counter
curl -s http://localhost:3001/api/eventual/page-views | jq .
# {"key":"page-views","value":{"type":"counter","value":1}}
```

### OR-Set operations

```bash
# Add element to a set
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"set_add","key":"tags","element":"important"}'

# Remove element
curl -s -X POST http://localhost:3001/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{"type":"set_remove","key":"tags","element":"important"}'
```

### Certified write

```bash
curl -s -X POST http://localhost:3001/api/certified/write \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "balance",
    "value": {"type":"register","value":"1000"},
    "on_timeout": "pending"
  }'
```

### Certified read (with proof)

```bash
curl -s http://localhost:3001/api/certified/balance | jq .
# Returns value + certification status + cryptographic proof bundle
```

### Check certification status

```bash
curl -s http://localhost:3001/api/status/balance | jq .
# {"key":"balance","status":"certified"}
```

### SLO budget

```bash
curl -s http://localhost:3001/api/slo | jq .
```

### Metrics

```bash
curl -s http://localhost:3001/api/metrics | jq .
```

## CLI

The `asteroidb-cli` binary provides operational commands:

```bash
# Build the CLI
cargo build --release --bin asteroidb-cli

# Node status summary
asteroidb-cli status

# Read a key
asteroidb-cli get sensor-1

# Write a register value
asteroidb-cli put sensor-1 "23.5"

# Detailed metrics
asteroidb-cli metrics

# SLO error budget
asteroidb-cli slo
```

Use `--host` or `ASTEROIDB_HOST` to target a specific node:

```bash
asteroidb-cli --host 127.0.0.1:3002 status
```

## Development

### Build and test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test                     # All tests
cargo test --lib               # Library unit tests only
cargo test <module>            # Specific module
```

### Lint and format

```bash
cargo fmt --check              # Check formatting
cargo fmt                      # Auto-format
cargo clippy -- -D warnings    # Lint (CI gate)
```

### CI gate (must pass before merge)

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

### Network simulation

```bash
# Lightweight netem scenarios (requires tc / NET_ADMIN)
scripts/test-netem-light.sh
```

## Project Structure

```
src/
  lib.rs                  # Library root
  main.rs                 # Binary entry point (HTTP server + NodeRunner)
  bin/cli.rs              # asteroidb-cli binary
  crdt/                   # CRDT implementations
    pn_counter.rs         #   PN-Counter
    or_set.rs             #   OR-Set
    or_map.rs             #   OR-Map + LWW-Register
    lww_register.rs       #   LWW-Register
  store/                  # Versioned KV storage with persistence
  authority/              # Consensus and certificate management
    ack_frontier.rs       #   HLC-based frontier tracking
    certificate.rs        #   Ed25519 / BLS dual-mode certificates
    bls.rs                #   BLS12-381 threshold signatures
  placement/              # Tag-based replica placement
    policy.rs             #   Placement policies
    latency.rs            #   Sliding-window RTT model
    topology.rs           #   Region-aware topology view
    rebalance.rs          #   Rebalance plan computation
  control_plane/          # System namespace and quorum consensus
  network/                # Peer management and delta sync
    membership.rs         #   Fan-out join/leave protocol
    sync.rs               #   Anti-entropy delta sync with backoff
  ops/                    # Operational tooling
    metrics.rs            #   Runtime metrics collection
    slo.rs                #   SLO framework and error budgets
  compaction/             # Log compaction engine
    engine.rs             #   Compaction with adaptive tuning
    tuner.rs              #   Write-rate tracker
  http/                   # HTTP API layer (Axum)
    routes.rs             #   Route definitions
    handlers.rs           #   Request handlers
    types.rs              #   Request/response types
    auth.rs               #   Bearer token middleware
  hlc.rs                  # Hybrid Logical Clock
  node.rs                 # Node definition
  error.rs                # Shared error types
  types.rs                # Shared type definitions
  runtime/                # NodeRunner background loops
docs/                     # Documentation
configs/                  # Per-node JSON configs for Docker
scripts/                  # Cluster management and test scripts
tests/                    # Integration / E2E tests
```

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Component design, data flows, and sequence diagrams |
| [Security](SECURITY.md) | Threat model, trust boundaries, and cryptographic primitives |
| [Vision](docs/vision.md) | Project goals and scope |
| [Requirements](docs/requirements.md) | MVP functional and non-functional requirements |

## License

This project is currently under development and not yet licensed for
distribution. A license will be selected before public release.
