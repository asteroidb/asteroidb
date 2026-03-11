# AsteroidDB Sample App: Collaborative Task Board

A full-stack Rust application demonstrating AsteroidDB's key features through an interactive Kanban task board.

## What This Demonstrates

### Dual Consistency Model
- **Eventual writes** for fast operations (create tasks, vote, tag, move to "doing")
- **Certified writes** when completing a task (move to "done"), showing the Pending → Certified lifecycle

### CRDT Types in Action
| Feature | CRDT Type | Why |
|---------|-----------|-----|
| Task metadata (title, desc) | OR-Map | Concurrent edits to different fields merge cleanly |
| Task status (todo/doing/done) | LWW-Register | Single-valued enum, last-write-wins |
| Vote count | PN-Counter | Multiple users voting simultaneously converge correctly |
| Tags | OR-Set | Concurrent add/remove resolved by OR-Set semantics |

### Certification & Proof Verification
- Real-time certification status polling (Pending → Certified/Rejected/Timeout)
- Proof bundle inspection showing frontier, authorities, and certificate
- Independent client-side proof verification

### Operational Dashboard
- Runtime metrics (pending certifications, latency, sync failure rate)
- SLO budget bars with warning/critical thresholds
- Cluster topology with region mapping and inter-region latency

## Architecture

```
Browser (Leptos WASM)  →  BFF Server (Axum)  →  AsteroidDB Cluster
      CSR app              /bff/api/*             /api/*
```

The BFF (Backend-For-Frontend) server:
- Serves the WASM frontend as static files
- Translates high-level task operations into CRDT-specific AsteroidDB API calls
- Provides cluster health aggregation across all nodes

## Prerequisites

- Rust nightly (automatically configured via `rust-toolchain.toml`)
- [Trunk](https://trunkrs.dev/) for WASM builds: `cargo install trunk`
- A running AsteroidDB cluster

## Quick Start

### 1. Start an AsteroidDB Cluster

```bash
# From the asteroidb root directory
cargo run -- --port 3001 --node-id node1 &
cargo run -- --port 3002 --node-id node2 --seed http://localhost:3001 &
cargo run -- --port 3003 --node-id node3 --seed http://localhost:3001 &
```

### 2. Build the Frontend

```bash
cd examples/sample-app-rust
trunk build
```

For development with hot-reload:
```bash
trunk serve --proxy-backend=http://localhost:8080/bff/api/
```

### 3. Start the BFF Server

```bash
cd examples/sample-app-rust
cargo run -- --nodes http://localhost:3001,http://localhost:3002,http://localhost:3003
```

### 4. Open the App

Navigate to [http://localhost:8080](http://localhost:8080)

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `BFF_PORT` | 8080 | BFF server port |
| `--nodes` | `ASTEROIDB_NODES` | http://localhost:3001 | Comma-separated AsteroidDB node URLs |
| `--static-dir` | `STATIC_DIR` | frontend/dist | Path to trunk build output |

## Project Structure

```
examples/sample-app-rust/
├── Cargo.toml              # Server crate (feature-gated deps)
├── Trunk.toml              # WASM build config
├── src/
│   ├── main.rs             # Axum server entry point
│   ├── config.rs           # CLI configuration (clap)
│   ├── proxy.rs            # AsteroidDB API proxy
│   ├── routes.rs           # BFF route definitions
│   ├── handlers.rs         # Request handlers
│   ├── error.rs            # Error types
│   └── shared/types.rs     # Shared DTOs (server + frontend)
└── frontend/
    ├── Cargo.toml           # Leptos CSR crate
    ├── index.html           # HTML shell
    ├── style/main.css       # Styles
    └── src/
        ├── lib.rs           # WASM entry point
        ├── app.rs           # Root component + router
        ├── api.rs           # BFF API client
        ├── state.rs         # Global reactive state
        └── components/      # UI components
```

## Testing the Dual Consistency Model

1. **Create a task** → Appears instantly (eventual write, no consensus needed)
2. **Vote on it** → Count updates immediately (PN-Counter, eventual)
3. **Add tags** → Tags appear instantly (OR-Set, eventual)
4. **Move to "Done"** → Shows "Pending" badge, then transitions to "Certified" as the cluster reaches consensus
5. **View Proof** → Expand the proof bundle to see frontier timestamps and authority signatures
6. **Verify** → Click "Verify Independently" to cryptographically verify the proof
