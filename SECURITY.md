# Security

This document describes the threat model, trust boundaries, and
cryptographic primitives used by AsteroidDB.

## Threat Model Overview

AsteroidDB's MVP is designed for **crash-fault tolerance**, not Byzantine
fault tolerance. The system assumes that all participating nodes are honest
but may fail by crashing or becoming unreachable. A compromised node that
deviates from the protocol can violate safety guarantees.

### In scope (MVP)

- **Crash faults**: nodes may stop, restart, or lose network connectivity
  at any time.
- **Network partitions**: links between nodes may fail transiently or for
  extended periods.
- **Replay attacks on inter-node API**: prevented by bearer token
  authentication when `ASTEROIDB_INTERNAL_TOKEN` is configured.
- **Unauthorized control-plane mutations**: protected by the same bearer
  token; only authenticated requests can modify placement policies and
  authority definitions.

### Out of scope (MVP)

- **Byzantine faults**: a malicious node can forge signatures, inject
  fabricated data, or produce invalid certificates. BFT extensions are
  planned for a future phase.
- **Client authentication/authorization**: the public HTTP API (reads and
  eventual writes) is unauthenticated. Applications should place AsteroidDB
  behind a reverse proxy or API gateway for client-facing deployments.
- **Encryption at rest**: data on disk is not encrypted. Use volume-level
  encryption (e.g., LUKS, dm-crypt) if needed.
- **TLS for inter-node traffic**: inter-node communication uses plain HTTP
  by default. Deploy behind a service mesh or configure a TLS terminator
  for production use.

## Trust Boundaries

```
+---------------------------+
|        Client Zone        |  Untrusted (no auth in MVP)
+------------+--------------+
             | HTTP
+------------v--------------+
|        Node (public API)  |  Reads, eventual writes, certified reads
+------------+--------------+
             | Internal API (bearer token)
+------------v--------------+
|     Node <-> Node         |  Delta sync, frontier exchange, join/leave
+---------------------------+
             | Internal API (bearer token)
+------------v--------------+
|     Control Plane         |  Policy mutations, authority definitions
+---------------------------+
```

### Boundary 1: Client to Node

- **Transport**: HTTP (no TLS by default).
- **Authentication**: None in MVP. All public endpoints are open.
- **Threat**: An attacker on the network can read and write data.
- **Mitigation**: Deploy behind a TLS-terminating reverse proxy with
  application-level auth for production.

### Boundary 2: Node to Node

- **Transport**: HTTP with optional bearer token.
- **Authentication**: When `ASTEROIDB_INTERNAL_TOKEN` is set, all
  `/api/internal/*` endpoints require `Authorization: Bearer <token>`.
  Without the token, inter-node routes are open.
- **Threat**: An attacker who obtains the internal token can join the
  cluster, inject data, or disrupt sync.
- **Mitigation**: Use a strong random token, rotate periodically, and
  restrict network access to cluster nodes.

### Boundary 3: Control Plane

- **Transport**: Same HTTP layer as node-to-node.
- **Authentication**: Mutation routes (`PUT /api/control-plane/policies`,
  `PUT /api/control-plane/authorities`, `DELETE ...`) require bearer token
  authentication.
- **Authorization**: Any request with a valid token can modify the control
  plane. There is no role-based access control in the MVP.
- **Threat**: Token compromise allows arbitrary policy changes (e.g.,
  reducing replica count, reassigning authority nodes).
- **Mitigation**: Limit token distribution, audit policy version history
  via `GET /api/control-plane/versions`.

## Cryptographic Primitives

| Primitive | Library | Usage |
|-----------|---------|-------|
| **Ed25519** | `ed25519-dalek 2.x` | Individual authority signatures for majority certificates |
| **BLS12-381** | `blst 0.3` | Aggregate threshold signatures; multiple authority sigs combine into one |
| **HLC** | Custom (`src/hlc.rs`) | Hybrid Logical Clock for causal ordering and frontier tracking |
| **SHA-256** | via `blst` DST | Domain separation tag for BLS signature scheme |

### Ed25519 Certificates

Each authority node holds an Ed25519 signing key. When a node acknowledges
an update, it signs the `(key_range, frontier_hlc, digest_hash)` tuple. A
majority certificate collects `n/2 + 1` individual signatures and their
corresponding verifying keys.

Verification: the client checks each signature against the authority's
public key and confirms that a majority of the declared authority set has
signed.

### BLS Aggregate Signatures

When BLS mode is enabled (`ASTEROIDB_BLS_SEED`), authority nodes produce
BLS12-381 signatures instead of Ed25519. Multiple BLS signatures over the
same message can be aggregated into a single signature, reducing certificate
size from O(n) to O(1) for n authorities.

Domain separation tag: `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_`

### Key Management

- **Keyset versioning**: Keys are managed in the system namespace under a
  monotonically increasing `keyset_version` (starting at 1).
- **Epoch rotation**: Default epoch length is 24 hours. On epoch boundary,
  the node switches to the next published keyset.
- **Grace period**: Verification accepts signatures from the current epoch
  and up to 7 past epochs, allowing for clock skew and delayed propagation.
- **Rotation procedure**:
  1. Publish next keyset to system namespace.
  2. Wait for epoch boundary -- nodes switch automatically.
  3. After grace period expires, old keys are invalidated.

## Authentication: Internal Token

AsteroidDB uses a shared-secret bearer token for inter-node and
control-plane authentication.

### Configuration

Set the `ASTEROIDB_INTERNAL_TOKEN` environment variable on all nodes with
the same value:

```bash
export ASTEROIDB_INTERNAL_TOKEN=$(openssl rand -hex 32)
```

### Protected routes

When the token is configured, the following routes require
`Authorization: Bearer <token>`:

- `/api/internal/*` -- sync, frontier exchange, join, leave, ping
- `PUT /api/control-plane/authorities`
- `PUT /api/control-plane/policies`
- `DELETE /api/control-plane/policies/{prefix}`

### Unprotected routes

Public API endpoints are always open:

- `GET /api/eventual/{key}`
- `POST /api/eventual/write`
- `GET /api/certified/{key}`
- `POST /api/certified/write`
- `GET /api/status/{key}`
- `GET /api/metrics`
- `GET /api/slo`
- `GET /api/topology`
- `GET /api/control-plane/authorities` (read)
- `GET /api/control-plane/policies` (read)
- `GET /api/control-plane/versions` (read)

## Known Limitations

1. **No Byzantine tolerance**: A compromised authority node can produce
   valid-looking signatures for arbitrary data. This is an explicit MVP
   scope boundary.

2. **Shared-secret token model**: All nodes share the same token. There is
   no per-node identity or mutual TLS. Token compromise gives full cluster
   access.

3. **No TLS**: All traffic is plain HTTP. Eavesdropping and MITM attacks
   are possible on untrusted networks.

4. **No client auth**: Any network-reachable client can read and write data
   via the public API.

5. **No audit logging**: There is no tamper-evident log of API requests.
   Policy version history provides partial auditability for control-plane
   changes.

6. **Clock dependency**: HLC depends on roughly synchronized clocks. Large
   clock skew (>> epoch length) can cause frontier tracking anomalies.

## Reporting Vulnerabilities

This project is in active development and not yet deployed in production.
If you discover a security issue, please open a GitHub issue or contact the
maintainers directly.
