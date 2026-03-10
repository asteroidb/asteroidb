# WASM Compatibility Audit

This document tracks C/C++ binding dependencies that block `wasm32-unknown-unknown`
and `wasm32-wasi` builds, and defines the feature-flag isolation strategy.

## Audit Date

2026-03-10 (Issue #295)

## Dependency Analysis

### C/C++ Binding Crates (WASM Blockers)

| Crate | Type | Why it blocks WASM | Pulled in by | Isolation |
|-------|------|-------------------|--------------|-----------|
| **blst 0.3** | C library (build.rs + cc) | Compiles C/assembly BLS12-381 implementation via `cc` crate | Direct dependency | `native-crypto` feature gate |
| **openssl-sys 0.9** | C library (build.rs + cc + pkg-config) | Links system OpenSSL | `reqwest` -> `native-tls` -> `openssl` | `native-tls` feature gate on reqwest |
| **openssl 0.10** | FFI bindings | Wraps openssl-sys | `reqwest` -> `native-tls` | Same as above |

### Crates Requiring WASM Configuration (Conditional Blockers)

| Crate | Issue | Mitigation |
|-------|-------|------------|
| **redb 2** | Uses `libc` for mmap/file I/O; no WASM target support | `native-storage` feature gate; use `InMemoryKvBackend` on WASM |
| **tokio 1 (full)** | `mio`, `signal`, `fs`, `net`, `process` require OS primitives | Use `wasm-bindgen-futures` or stripped tokio feature set on WASM |
| **reqwest 0.12** | HTTP client uses OS networking stack | Replace with `fetch` API via `wasm-bindgen` on WASM targets |
| **getrandom 0.2** | Needs OS entropy source; supports WASM with `js` feature | Enable `getrandom/js` feature for `wasm32-unknown-unknown` |
| **cpufeatures 0.2** | CPU feature detection; gracefully no-ops on unknown targets | No action needed (returns empty feature set on WASM) |

### Pure-Rust Crates (WASM Compatible)

These direct dependencies require **no changes** for WASM:

- `ed25519-dalek` -- pure Rust curve25519-dalek backend; WASM-compatible
- `serde` / `serde_json` / `bincode` -- pure Rust serialization
- `clap` -- pure Rust CLI parser (not useful on WASM but compiles)
- `thiserror` -- proc-macro only
- `tracing` / `tracing-subscriber` -- pure Rust (subscriber may need WASM time source)
- `hex` / `subtle` / `siphasher` -- pure Rust

## Feature-Flag Isolation Strategy

### Feature Definitions

```toml
[features]
default = ["native-crypto", "native-tls", "native-storage", "native-runtime"]

# BLS12-381 signatures via blst (C library). Disable for WASM.
native-crypto = ["dep:blst"]

# System TLS via openssl/native-tls for reqwest. Disable for WASM.
native-tls = ["reqwest/default-tls"]

# Persistent storage via redb. Disable for WASM (use InMemoryKvBackend).
native-storage = ["dep:redb"]

# Full tokio runtime with OS primitives (net, fs, signal).
native-runtime = ["tokio/full"]

# Enable WASM-compatible dependencies.
wasm = ["getrandom/js"]
```

### Code Isolation Pattern

#### BLS Module (`src/authority/bls.rs`)

The entire `bls` module is gated behind `#[cfg(feature = "native-crypto")]`.
When disabled, `DualModeCertificate` in `certificate.rs` falls back to
Ed25519-only mode (already the default for MVP).

```rust
// src/authority/mod.rs
#[cfg(feature = "native-crypto")]
pub mod bls;
```

#### Redb Backend (`src/store/backend.rs`)

The `RedbBackend` struct and its imports are gated behind
`#[cfg(feature = "native-storage")]`. `InMemoryKvBackend` and `FileBackend`
remain always available (FileBackend requires `std::fs`, which is available
on `wasm32-wasi` but not `wasm32-unknown-unknown`).

```rust
#[cfg(feature = "native-storage")]
pub struct RedbBackend { ... }
```

#### Reqwest TLS (`Cargo.toml`)

Reqwest's `default-tls` feature is moved behind the `native-tls` feature
flag. For WASM builds, reqwest would not be included (HTTP client must be
replaced with fetch-based implementation).

### Module Dependency Map

```
                    ┌─────────────────────┐
                    │     lib.rs          │
                    └────┬────────────────┘
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
┌─────────┐      ┌────────────┐      ┌──────────┐
│authority/│      │  store/    │      │ network/ │
│          │      │            │      │          │
│ bls.rs ◄─┤      │ backend.rs │      │ sync.rs  │
│ [native- │      │            │      │[reqwest] │
│  crypto] │      │ RedbBackend│      └──────────┘
│          │      │ [native-   │
│cert.rs   │      │  storage]  │
└──────────┘      └────────────┘
```

### Build Commands

```bash
# Default (native) build -- unchanged behavior
cargo build

# WASM-compatible build (no C/C++ deps)
cargo build --target wasm32-unknown-unknown \
  --no-default-features --features wasm

# Check WASM compatibility
cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm
```

## Migration Roadmap

### Phase 1: Feature Gates (This PR)

- Add feature flags to `Cargo.toml`
- Gate `blst` dep and `bls` module behind `native-crypto`
- Gate `RedbBackend` behind `native-storage`
- Gate reqwest TLS behind `native-tls`
- Ensure `cargo check` and `cargo test` pass with default features

### Phase 2: WASM Runtime (Future)

- Add `wasm-bindgen-futures` as alternative async runtime
- Implement fetch-based HTTP client for WASM
- Add `wasm32-unknown-unknown` to CI matrix
- Gate `FileBackend` behind `cfg(not(target_arch = "wasm32"))` or `std::fs` availability

### Phase 3: Full WASM Support (Future)

- Publish `asteroidb-wasm` npm package
- WASM-compatible time source for tracing/HLC
- Browser-based demo
