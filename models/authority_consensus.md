# Authority Consensus Model

This document describes the consensus invariants for AsteroidDB's Authority
majority consensus protocol. It serves as a lightweight TLA+-style specification
documenting the safety and liveness properties that the system must uphold.

## State Space

### Variables

| Variable | Type | Description |
|----------|------|-------------|
| `Nodes` | Set of NodeId | The set of all authority nodes in a key range scope |
| `N` | Nat | Total number of authority nodes (`|Nodes|`) |
| `Keys` | Set of KeyRange | Key ranges under authority governance |
| `Frontiers` | NodeId -> HlcTimestamp | Per-authority ack frontiers for a given (key_range, policy_version) |
| `Certificates` | Set of MajorityCertificate | Issued majority certificates |
| `PolicyVersion` | Nat | Current placement policy version |
| `KeysetVersion` | Nat | Current keyset version for signing |

### Constants

- `Majority = floor(N / 2) + 1` — strict majority threshold

### Actions

1. **UpdateFrontier(auth, hlc)**: Authority `auth` advances its frontier to `hlc`.
   - Precondition: `hlc > Frontiers[auth]`
   - Effect: `Frontiers[auth] = hlc`

2. **IssueCertificate(key_range, signers, frontier_hlc)**:
   - Precondition: `|signers| >= Majority` and all `signers` are in `Nodes`
   - Each signer's frontier >= `frontier_hlc`
   - Effect: A new `MajorityCertificate` is added to `Certificates`

3. **FenceVersion(key_range, policy_version)**:
   - Effect: No further frontier updates accepted for the fenced pair

## Safety Properties

### S1: No Conflicting Certifications (Quorum Intersection)

**Invariant**: For any two valid majority certificates `C1` and `C2` covering
the same `key_range` and `policy_version`, the sets of signers must intersect:

```
forall C1, C2 in Certificates:
  C1.key_range = C2.key_range /\ C1.policy_version = C2.policy_version
  => C1.signers ∩ C2.signers ≠ {}
```

**Why it holds**: Both certificates require `>= floor(N/2) + 1` distinct
signers from the same set of `N` nodes. By the pigeonhole principle, two
subsets of size `>= N/2 + 1` drawn from a set of `N` elements must have
at least `(N/2 + 1) + (N/2 + 1) - N = 2` common elements when `N` is odd,
or at least `1` when `N` is even.

### S2: Frontier Monotonicity

**Invariant**: A frontier for a given `(key_range, policy_version, authority_id)`
scope never regresses:

```
forall scope:
  Frontiers'[scope] >= Frontiers[scope]
```

**Why it holds**: The `AckFrontierSet::update()` method only accepts updates
where the new HLC timestamp is strictly greater than the existing one.

### S3: Majority Frontier Consistency

**Invariant**: If a timestamp `t` is certified (i.e., at least `Majority`
authorities have frontier `>= t`), then `t` remains certified as frontiers
can only advance:

```
once_certified(t) => always_certified(t)
```

**Why it holds**: Combines S2 (monotonicity) with the fact that once an
authority reaches `t`, it cannot go below `t`.

### S4: Type Safety on Merge

**Invariant**: A `Store::merge_value` operation for a key with an existing
value of type `T` only succeeds if the incoming value is also of type `T`:

```
forall key, incoming:
  store.get(key) = Some(existing) /\ type(existing) != type(incoming)
  => merge_value returns Err(TypeMismatch)
```

## Liveness Properties

### L1: Majority Reachable Implies Certification Succeeds

**Property**: If a strict majority of authority nodes are reachable (i.e.,
can process updates and sign certificates), then certification for any
pending write will eventually succeed:

```
<>(majority_reachable) => <>(certification_complete)
```

**Assumption**: Fair scheduling — each reachable authority eventually processes
pending updates.

### L2: Frontier Progress

**Property**: If an authority node is operational and receiving updates,
its frontier eventually advances:

```
operational(auth) /\ updates_arriving => <>(Frontiers[auth] increases)
```

### L3: Convergence Under Partition Healing

**Property**: After a network partition heals and bidirectional CRDT merge
completes, all replicas converge to the same state:

```
partition_healed /\ merge_complete
=> forall r1, r2 in replicas: state(r1) = state(r2)
```

**Why it holds**: All CRDT types (PnCounter, OrSet, OrMap, LwwRegister)
satisfy commutativity, associativity, and idempotency of their merge
operations, as verified by the property tests in `tests/property_crdt.rs`.

## Relationship to Property Tests

| Property | Test File | Test Name |
|----------|-----------|-----------|
| S1 (Quorum Intersection) | `tests/property_quorum.rs` | `majority_certificate_signer_overlap` |
| S1 (Partition Safety) | `tests/property_quorum.rs` | `quorum_intersection_partition` |
| S1 (Set Intersection) | `tests/property_quorum.rs` | `two_majorities_intersect` |
| CRDT Commutativity | `tests/property_crdt.rs` | `*_commutativity` |
| CRDT Associativity | `tests/property_crdt.rs` | `*_associativity` |
| CRDT Idempotency | `tests/property_crdt.rs` | `*_idempotency` |
| L3 (Store Convergence) | `tests/property_store.rs` | `store_bidirectional_merge_converges` |
