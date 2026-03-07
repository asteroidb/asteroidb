# Disaster Recovery Runbook

## Data Loss Scenarios

### Scenario 1: Single Node Data Loss

**Cause**: Disk failure, accidental deletion, or corruption on one node.

**Impact**: Eventual data on the failed node is lost. Certified data may
be affected if the node was an Authority.

**Recovery**:

1. Restart the node with a clean data directory.
2. The node will re-join the cluster via fan-out join.
3. Anti-entropy sync will automatically replicate all data from peers.
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
   new replacement nodes:

   ```bash
   curl -X PUT http://seed:3000/api/control-plane/authorities \
     -H 'Content-Type: application/json' \
     -d '{
       "key_range_prefix": "",
       "authority_nodes": ["auth-1", "auth-new-2", "auth-new-3"],
       "approvals": ["auth-1"]
     }'
   ```

   Note: This requires at least one surviving Authority node for approval.

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
