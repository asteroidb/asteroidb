# Disaster Recovery Runbook

## Data Loss Scenarios

### Scenario 1: Single Node Data Loss

**Cause**: Disk failure, accidental deletion, or corruption on one node.

**Impact**: Eventual data on the failed node is lost. Certified data may
be affected if the node was an Authority.

**Recovery**:

> **WARNING**: Starting "with a clean data directory" is only acceptable
> when the disk itself was lost (nothing salvageable remains). If only part
> of the data directory is corrupted, **never move aside or delete the
> whole directory**. Only the *eventual* store can be rebuilt from peers;
> the certified store has no anti-entropy rebuild path (discarding it is
> permanent loss), wiping `raft/` re-introduces double-voting risk in an
> already-voted term, and discarding `equivocation_evidence.json` destroys
> evidence permanently. For partial corruption, follow the crash-recovery
> runbook in `docs/ops-guide.md` §13.6 and move aside only the damaged
> store's files.

1. Restart the node (with a clean data directory only if the disk itself was
   lost; otherwise follow ops-guide §13.6 and preserve `raft/`,
   `equivocation_evidence.json`, and the certified snapshot/WAL).
2. The node will re-join the cluster via fan-out join.
3. Anti-entropy sync will automatically replicate all *eventual* data from
   peers. Certified data and Raft state have no peer-rebuild path — restore
   them from backup if they were lost.
4. Monitor convergence:

   ```bash
   asteroidb-cli --host recovered-node:3000 slo
   ```

5. Full convergence is achieved when `replication_convergence` SLO shows
   zero violations.

### Scenario 2: Minority Authority Loss

**Cause**: Fewer than half of Authority nodes are unavailable.

**Impact**: Certified writes continue (majority still available). No data
loss, but latency may increase.

**Recovery**:

1. Restore the failed Authority nodes.
2. They will catch up via frontier sync automatically.
3. Verify Authority availability:

   ```bash
   asteroidb-cli --host any-node:3000 slo
   ```

### Scenario 3: Majority Authority Loss

**Cause**: More than half of Authority nodes are unavailable simultaneously.

**Impact**: Certified writes will fail or timeout. Eventual writes continue
normally.

**Recovery**:

1. **Priority 1**: Restore Authority nodes to regain majority. This is the
   fastest recovery path.

2. If nodes cannot be restored, update the Authority definition to include
   new replacement nodes. The update is committed through the control-plane
   Raft log, so it must be sent to the **current Raft leader** (any other
   node answers `503 NOT_LEADER` with `x-asteroidb-leader-id` /
   `x-asteroidb-leader-addr` hint headers — retry against the leader):

   ```bash
   # Find the leader
   curl -s http://seed:3000/api/control-plane/raft/status | jq '{role, leader_id, leader_addr}'

   curl -X PUT http://<leader-addr>/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["auth-1", "auth-new-2", "auth-new-3"]
     }'
   ```

   Note: This requires a majority of the control-plane Raft voter set
   (`ASTEROIDB_CONTROL_PLANE_NODES`) to be up and reachable — Raft cannot
   commit without a quorum. The deprecated `approvals` field is accepted
   but ignored. If the Raft quorum itself is lost, see
   `docs/ops-guide.md` §14 before proceeding.

3. If ALL Authority nodes are lost, manual intervention is required:
   - Stop all remaining nodes.
   - Edit the system namespace configuration directly.
   - Define new Authority nodes.
   - Restart the cluster.
   - Re-certify all pending writes.

### Scenario 4: Complete Cluster Loss

**Cause**: All nodes are unavailable (catastrophic failure).

**Recovery**:

1. This scenario requires backup restoration. Ensure regular backups of:
   - `$ASTEROIDB_DATA_DIR/` on each node.
   - Peer registry files (`peers.json`).
   - System namespace configuration.

2. Restore from the most recent backup:

   ```bash
   # On each node:
   cp /backup/data/* $ASTEROIDB_DATA_DIR/
   ```

3. Start seed node first, then other nodes.

4. Verify data integrity across the cluster:

   ```bash
   for node in node1:3000 node2:3001 node3:3002; do
     asteroidb-cli --host $node status
   done
   ```

## Prevention

- **Replication**: Use replica_count >= 3 in placement policies.
- **Authority redundancy**: Deploy at least 3 Authority nodes per key range.
- **Monitoring**: Set up alerts on SLO budget consumption:
  - Warning at 50% budget consumed.
  - Critical at 80% budget consumed.
- **Backups**: Regular snapshots of data directories.
- **Geographic distribution**: Spread nodes across failure domains.
