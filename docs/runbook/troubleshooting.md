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
