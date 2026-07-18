# Troubleshooting Runbook

## Split-Brain Detection

### Symptoms

- Different nodes return different values for the same key.
- `frontier_skew_ms` metric is abnormally high (>10000ms).
- Sync failure rate increases significantly.

### Diagnosis

1. Check frontier skew across nodes:

   ```bash
   for node in 10.0.1.1:3000 10.0.1.2:3001 10.0.1.3:3002; do
     echo "=== $node ==="
     asteroidb-cli --host $node metrics | grep frontier_skew
   done
   ```

2. Check sync failure rates:

   ```bash
   asteroidb-cli --host 10.0.1.1:3000 status
   ```

3. Verify network connectivity between nodes:

   ```bash
   curl -s http://10.0.1.1:3000/api/internal/ping \
     -H 'Content-Type: application/json' \
     -d '{"sender_id":"probe","sender_addr":"","known_peers":[]}'
   ```

### Resolution

1. If caused by network partition: restore connectivity. CRDT merge will
   automatically converge once sync resumes.

2. If caused by clock drift: check NTP synchronization on affected nodes.
   The HLC compensates for moderate drift, but extreme drift (>10s) may
   cause ordering anomalies.

3. Force a full sync from a known-good node:

   ```bash
   curl http://good-node:3000/api/internal/keys > /tmp/dump.json
   curl -X POST http://bad-node:3001/api/internal/sync \
     -H 'Content-Type: application/json' \
     -d @/tmp/dump.json
   ```

## Authority Equivocation Detected

### Symptoms

- `equivocation_detected_total` metric is greater than 0 (P1 — respond
  immediately).
- `EQUIVOCATION DETECTED` warning in the logs, with structured fields
  (`authority`, `key_range`, `digest_first`, `digest_second`).
- `equivocation_accused_authorities` gauge is non-zero.
- A `self-attestation equivocation` warning indicates this node's *own*
  signing key produced conflicting reports — a strong signal of key
  compromise or two processes misconfigured with the same key seed.

### Diagnosis

1. Fetch the evidence bundle from the detecting node:

   ```bash
   curl -s http://node:3000/api/authority/equivocations | jq .
   ```

   Each evidence entry contains **both conflicting signed attestations
   verbatim** (`first` / `second`, hex signatures included). The pair is a
   non-repudiable proof of misbehaviour: the report signature covers every
   frontier field including `digest_hash`, so two valid signatures over the
   same `(authority, key_range, policy_version, frontier_hlc)` with
   different digests cannot both be honest.

2. Re-verify both report signatures offline against the registry key for
   the accused authority (the key distributed via
   `ASTEROIDB_AUTHORITY_KEYS`). If either signature does not verify, the
   evidence is invalid and must be discarded — the detector only records
   verified pairs, so this indicates tampering with the evidence file.

3. Cross-check other nodes: query `/api/authority/equivocations` on every
   node. Evidence propagates via the frontier-push gossip lane, so multiple
   nodes typically converge on the same accusation within a few report
   ticks.

4. Distinguish key compromise from a malicious operator:
   - Check the accused authority's own logs for `self-attestation
     equivocation` warnings (suggests a leaked key or duplicated seed).
   - Check whether two processes were started with the same
     `ASTEROIDB_BLS_SEED` (a common misconfiguration that produces genuine
     conflicting signatures without malice).

### Resolution

1. **Preserve the evidence.** Save the JSON response and the node's
   `equivocation_evidence.json` (in `ASTEROIDB_DATA_DIR`). The evidence is
   third-party verifiable and survives restarts, but keep an offline copy
   before any remediation.

2. **No automatic quarantine happens** — by design. Detection is local and
   cheap; *excluding* an authority is an enforcement decision that
   requires cluster-level agreement, so the node only warns, records and
   reports. Do not expect the cluster to fence the authority on its own.

3. Optional local mitigation: set `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES=1`
   and restart. The node then drops the accused authority's attestations
   from certificate assembly (frontiers still advance; the majority
   denominator is unchanged, so this is always fail-safe). **Availability
   cost**: if exclusion drops a scope below majority, certificate
   production for that scope stalls until the authority set is fixed.

4. Permanent removal: update the authority set through the control-plane
   Raft consensus. Send the request to the **current Raft leader** — any
   other node answers `503 NOT_LEADER` with leader hint headers
   (`x-asteroidb-leader-id` / `x-asteroidb-leader-addr`); retry against the
   hinted leader. The deprecated `approvals` field is ignored.

   ```bash
   # Find the leader
   curl -s http://node:3000/api/control-plane/raft/status | jq '{role, leader_id, leader_addr}'

   curl -X PUT http://<leader-addr>/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -d '{"key_range_prefix":"", "authority_nodes":["auth-1","auth-2"]}'
   ```

5. If the cause is key compromise rather than malice, rotate the affected
   key (see `key-rotation.md`) and redistribute
   `ASTEROIDB_AUTHORITY_KEYS`.

### Limitations to keep in mind

- Detection is **reactive**, not preventive: conflicting reports may have
  been accepted and distributed before detection.
- A colluding *majority* that tells every node the same lie is
  undetectable by this mechanism.
- The local observation window is ~2 minutes (128 entries per scope);
  conflicts against older heads are only caught if another node still
  gossips them. Recorded evidence itself is never evicted.

## Compaction Issues

### Symptoms

- Disk usage growing unbounded.
- Compaction metrics show zero progress:
  - `rebalance_complete_total` not increasing.

### Diagnosis

1. Check compaction metrics:

   ```bash
   asteroidb-cli --host node:3000 metrics
   ```

   Look for `rebalance_start_total`, `rebalance_complete_total`, and
   `rebalance_keys_failed`.

2. A high `rebalance_keys_failed` count indicates compaction is attempting
   but failing on specific keys.

### Resolution

1. Verify that Authority nodes are reachable. Compaction requires majority
   Authority ack_frontier confirmation (FR-010).

2. If Authority nodes are down, compaction will stall. Restore Authority
   availability first.

3. For stuck compaction, restart the affected node. The compaction engine
   will resume from its last checkpoint.

## Sync Lag

### Symptoms

- Eventual reads return stale data.
- `sync_failure_rate` > 0.1 (10%).
- Peer sync P99 latency exceeds SLO targets.

### Diagnosis

1. Check per-peer sync statistics:

   ```bash
   asteroidb-cli --host node:3000 metrics
   ```

   Look at the `peer_sync` section for per-peer latency and failure counts.

2. Check SLO budget for replication convergence:

   ```bash
   asteroidb-cli --host node:3000 slo
   ```

3. Identify the lagging peer from the metrics output.

### Resolution

1. **Network issues**: Check connectivity to the lagging peer. High latency
   or packet loss will cause sync failures.

2. **Overloaded peer**: If a peer is slow, check its CPU and memory usage.
   Consider scaling horizontally.

3. **Delta sync fallback**: If `sync_fallback_total` is high, peers are
   frequently falling back to full sync. This indicates the delta window
   is too small for the write rate. Consider increasing the sync frequency
   in `NodeRunnerConfig`.

4. **Manual full sync**: Force a complete data pull:

   ```bash
   curl http://source:3000/api/internal/keys | \
     curl -X POST http://target:3001/api/internal/sync \
       -H 'Content-Type: application/json' -d @-
   ```
