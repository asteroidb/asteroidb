# Changelog

## v0.1.0 (2026-03-08)

Initial release.

### Core Features

- Dual consistency model: eventual (CRDT-based) and certified (authority majority)
- CRDT types: PnCounter, LWW-Register, OR-Set, OR-Map
- Hybrid Logical Clock (HLC) for causal ordering
- Delta-based anti-entropy sync with batching and backoff
- BLS12-381 threshold signatures with epoch-based key rotation
- Ed25519/BLS dual-mode certificates
- Placement policies with tag matching and latency-aware ranking
- Adaptive compaction with write-rate tracking
- SLO framework with error budget calculation

### Operations

- CLI tool (asteroidb-cli): status, get, put, metrics, slo
- Docker Compose 3-node cluster
- Fault injection and netem test scripts
- Criterion micro-benchmarks
- Multi-node benchmark scripts

### Security

- Constant-time bearer token authentication
- SSRF protection on internal endpoints
- Peer address validation
- Ping anti-poisoning (known sender + rate limit)
