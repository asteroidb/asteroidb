# WASM Compatibility

This document tracks the feature-flag isolation strategy for building
AsteroidDB on `wasm32-unknown-unknown`.

## Quick Start

```bash
# Default (native) build -- unchanged behavior
cargo build

# WASM-compatible build (no C/C++ deps, no OS primitives)
cargo build --target wasm32-unknown-unknown \
  --no-default-features --features wasm

# Check WASM compatibility (faster, no codegen)
cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `native-crypto` | yes | BLS12-381 signatures via `blst` (C library) |
| `native-tls` | yes | System TLS via OpenSSL for `reqwest` |
| `native-storage` | yes | Persistent storage via `redb` (libc mmap) |
| `native-runtime` | yes | Tokio runtime, Axum HTTP server, reqwest HTTP client, clap CLI, tracing-subscriber |
| `wasm` | no | Enables `getrandom/js` for WASM entropy; excludes all native features |

### Feature Definitions (Cargo.toml)

```toml
[features]
default = ["native-crypto", "native-tls", "native-storage", "native-runtime"]

native-crypto = ["dep:blst"]
native-tls = ["reqwest/default-tls"]
native-storage = ["dep:redb"]
native-runtime = ["dep:tokio", "dep:axum", "dep:reqwest", "dep:clap", "dep:tracing-subscriber"]
wasm = ["getrandom/js"]
```

## Module Availability on WASM

### Always available (core library)

These modules compile on `wasm32-unknown-unknown` with no feature flags:

- `crdt/` -- all CRDT types (PnCounter, OrSet, OrMap, LwwRegister, GC)
- `hlc` -- Hybrid Logical Clock
- `types` -- shared type definitions
- `error` -- error types
- `node` -- node definitions
- `authority/certificate` -- Ed25519 certificates (BLS falls back to stub)
- `authority/ack_frontier` -- frontier tracking (save/load gated)
- `authority/verifier` -- certificate verification
- `api/certified` -- certified API logic
- `api/eventual` -- eventual API logic
- `api/status` -- certification tracker (save/load gated)
- `compaction/` -- compaction engine and tuner
- `control_plane/` -- system namespace and consensus (save/load gated)
- `placement/` -- placement policy, latency, topology, rebalance
- `store/` -- Store, MemoryBackend, InMemoryKvBackend (FileBackend gated)
- `ops/diagnostics` -- diagnostic snapshots

### Requires `native-runtime`

These modules use tokio, axum, reqwest, or OS networking:

- `http/` -- Axum HTTP server (routes, handlers, auth, codec)
- `network/` -- peer management, delta sync, frontier sync, membership
- `runtime/` -- NodeRunner background loop
- `ops/metrics` -- per-peer sync stats (uses `std::time::Instant`)
- `ops/slo` -- SLO framework (uses `std::time::Instant`)

### File I/O Functions (gated with `cfg(not(target_arch = "wasm32"))`)

These specific functions are unavailable on WASM:

- `Store::save_snapshot`, `Store::load_snapshot`, `Store::load_snapshot_or_default`
- `CertificationTracker::save`, `CertificationTracker::load`
- `AckFrontierSet::save`, `AckFrontierSet::load`
- `SystemNamespace::save`, `SystemNamespace::load`
- `FileBackend` (entire struct)

Use `Store::save_to_backend` / `Store::load_from_backend` with `MemoryBackend`
on WASM instead.

### Requires `native-crypto`

- `authority/bls` -- BLS12-381 threshold signatures (uses `blst` C library)
- When disabled, `DualModeCertificate` uses stub types and BLS verification returns an error

### Requires `native-storage`

- `store/backend::RedbBackend` -- persistent KV backend (uses `redb` / libc mmap)
- Use `InMemoryKvBackend` on WASM instead

## Binaries

Both binaries require `native-runtime`:

- `asteroidb` (main server) -- `required-features = ["native-runtime"]`
- `asteroidb-cli` -- `required-features = ["native-runtime"]`

## Dependency Analysis

### C/C++ Binding Crates (isolated behind features)

| Crate | Feature Gate | Description |
|-------|-------------|-------------|
| `blst 0.3` | `native-crypto` | BLS12-381 via C/assembly |
| `openssl-sys 0.9` | `native-tls` | System OpenSSL (via reqwest) |
| `redb 2` | `native-storage` | Persistent KV via libc mmap |

### Runtime Crates (isolated behind `native-runtime`)

| Crate | Description |
|-------|-------------|
| `tokio 1` | Async runtime with OS primitives |
| `axum 0.8` | HTTP server framework |
| `reqwest 0.12` | HTTP client |
| `clap 4` | CLI argument parser |
| `tracing-subscriber 0.3` | Logging subscriber with env-filter |

### WASM-specific Dependencies

| Crate | Feature | Description |
|-------|---------|-------------|
| `getrandom 0.2` | `js` (via `wasm` feature) | Entropy via `crypto.getRandomValues()` |

### Pure-Rust Crates (always WASM-compatible)

- `ed25519-dalek` -- pure Rust Ed25519
- `serde` / `serde_json` / `bincode` -- serialization
- `thiserror` -- error derive macro
- `tracing` -- structured logging facade (no subscriber needed for compilation)
- `hex` / `subtle` / `siphasher` -- utilities
- `rand` -- random number generation (needs `getrandom/js` on WASM)

## CI

The WASM build is checked in CI:

```yaml
wasm:
  name: WASM Build Check
  steps:
    - uses: dtolnay/rust-toolchain@stable
      with:
        targets: wasm32-unknown-unknown
    - run: cargo check --target wasm32-unknown-unknown --no-default-features --features wasm
```

## Architecture

```
                    +---------------------+
                    |      lib.rs         |
                    +----+-------+--------+
                         |       |
    +--------------------+       +--------------------+
    |                    |                            |
    v                    v                            v
+----------+     +-----------+     +---------+  +----------+
| crdt/    |     | authority/|     | store/  |  | placement|
| (always) |     |           |     |         |  | (always) |
+----------+     | bls       |     | File    |  +----------+
                 | [native-  |     | Backend |
                 |  crypto]  |     | [!wasm] |
                 |           |     |         |
                 | cert/     |     | Redb    |
                 | verifier  |     | Backend |
                 | (always)  |     | [native-|
                 +-----------+     | storage]|
                                   +---------+

    Gated behind native-runtime:
    +----------+  +-----------+  +----------+
    |  http/   |  | network/  |  | runtime/ |
    |  (axum)  |  | (reqwest) |  | (tokio)  |
    +----------+  +-----------+  +----------+
```
