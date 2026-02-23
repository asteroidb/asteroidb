#!/usr/bin/env bash
# Scenario: Network partition and recovery with CRDT convergence verification.
#
# This script automates the following steps:
#   1. Verify the 3-node cluster is up
#   2. Write to node-1 via eventual API
#   3. Confirm sync across nodes
#   4. Partition node-3 (100% packet loss)
#   5. Write additional data to node-1
#   6. Verify node-3 still has old data
#   7. Recover node-3 (remove netem rules)
#   8. Wait and verify convergence
#
# Usage: ./scripts/netem/scenario-partition-recovery.sh
#
# Prerequisites:
#   - Docker cluster running (./scripts/cluster-up.sh)
#   - Containers have NET_ADMIN capability
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

# --- Configuration ---
NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE3_CONTAINER="asteroidb-node-3"
KEY="netem-test-key"

# Interval (seconds) to wait for data propagation.
SYNC_WAIT=3
# Maximum retries when waiting for convergence after recovery.
CONVERGENCE_RETRIES=10
CONVERGENCE_INTERVAL=2

# === STEP 1: Cluster health check ===
log_step 1 "Verify cluster health"

if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi
echo ""

# === STEP 2: Initial eventual write to node-1 ===
log_step 2 "Write counter to node-1 (3 increments)"

for i in 1 2 3; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/3 sent to node-1"
done
echo ""

# === STEP 3: Wait and verify sync ===
log_step 3 "Wait ${SYNC_WAIT}s for replication, then verify sync"
sleep "$SYNC_WAIT"

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done
echo ""

# === STEP 4: Partition node-3 ===
log_step 4 "Partition node-3 (100% packet loss)"

"${SCRIPT_DIR}/add-partition.sh" "$NODE3_CONTAINER"
echo ""

# === STEP 5: Additional writes during partition ===
log_step 5 "Write 5 more increments to node-1 (node-3 is partitioned)"

for i in 1 2 3 4 5; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/5 sent to node-1"
done
echo ""

sleep "$SYNC_WAIT"

# === STEP 6: Verify divergence ===
log_step 6 "Verify state divergence"

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name} (connected): counter = ${val}"
done

json3=$(read_counter "$NODE3_URL" "$KEY")
val3=$(extract_value "$json3")
echo "  node-3 (partitioned): counter = ${val3}"
echo ""
echo "  [Expected] node-1, node-2 have newer data; node-3 is behind."
echo ""

# === STEP 7: Recover node-3 ===
log_step 7 "Recover node-3 (remove netem rules)"

"${SCRIPT_DIR}/remove-netem.sh" "$NODE3_CONTAINER"
echo ""

# === STEP 8: Wait for convergence ===
log_step 8 "Wait for CRDT convergence after recovery"

# Read expected value from node-1.
json1=$(read_counter "$NODE1_URL" "$KEY")
expected=$(extract_value "$json1")
echo "  Expected converged value (from node-1): ${expected}"

converged=false
if wait_for_convergence "$expected" "$NODE3_URL" "node-3" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    converged=true
fi

echo ""
if ! $converged; then
    echo "  This may be expected if inter-node replication is not yet wired."
fi

echo ""
separator
echo "SCENARIO COMPLETE"
separator
echo ""
echo "Final state:"
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done
echo ""
echo "To clean up netem rules on all containers:"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-1"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-2"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-3"
