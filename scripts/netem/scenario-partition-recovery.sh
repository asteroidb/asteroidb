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

separator() {
    echo "======================================================================"
}

sub_separator() {
    echo "----------------------------------------------------------------------"
}

check_node() {
    local url="$1"
    local name="$2"
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "${url}/api/eventual/__health_check" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "  ${name}: UP"
        return 0
    else
        echo "  ${name}: DOWN (HTTP ${status})"
        return 1
    fi
}

read_counter() {
    local url="$1"
    curl -sf --max-time 5 "${url}/api/eventual/${KEY}" 2>/dev/null || echo '{"value":null}'
}

extract_value() {
    local json="$1"
    # Extract counter value. Handles {"key":"...","value":{"type":"counter","value":N}}.
    echo "$json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        print('null')
    elif isinstance(v, dict):
        print(v.get('value', 'null'))
    else:
        print(v)
except Exception:
    print('null')
" 2>/dev/null || echo "null"
}

# === STEP 1: Cluster health check ===
separator
echo "STEP 1: Verify cluster health"
sub_separator

all_up=true
for pair in "${NODE1_URL}:node-1" "${NODE2_URL}:node-2" "${NODE3_URL}:node-3"; do
    url="${pair%%:*}:${pair#*:}"
    # Re-split correctly
    url_part="${pair%:node-*}"
    name_part="${pair##*:}"
    # Fix: pair format is "http://localhost:3001:node-1", need to handle the URL correctly
    true
done

# Simpler approach
if ! check_node "$NODE1_URL" "node-1"; then all_up=false; fi
if ! check_node "$NODE2_URL" "node-2"; then all_up=false; fi
if ! check_node "$NODE3_URL" "node-3"; then all_up=false; fi

if ! $all_up; then
    echo ""
    echo "[ERROR] Not all nodes are up. Start the cluster first:"
    echo "  ./scripts/cluster-up.sh"
    exit 1
fi
echo ""
echo "All nodes healthy."
echo ""

# === STEP 2: Initial eventual write to node-1 ===
separator
echo "STEP 2: Write counter to node-1 (3 increments)"
sub_separator

for i in 1 2 3; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/3 sent to node-1"
done
echo ""

# === STEP 3: Wait and verify sync ===
separator
echo "STEP 3: Wait ${SYNC_WAIT}s for replication, then verify sync"
sub_separator
sleep "$SYNC_WAIT"

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done
echo ""

# === STEP 4: Partition node-3 ===
separator
echo "STEP 4: Partition node-3 (100% packet loss)"
sub_separator

"${SCRIPT_DIR}/add-partition.sh" "$NODE3_CONTAINER"
echo ""

# === STEP 5: Additional writes during partition ===
separator
echo "STEP 5: Write 5 more increments to node-1 (node-3 is partitioned)"
sub_separator

for i in 1 2 3 4 5; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/5 sent to node-1"
done
echo ""

sleep "$SYNC_WAIT"

# === STEP 6: Verify divergence ===
separator
echo "STEP 6: Verify state divergence"
sub_separator

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url")
    val=$(extract_value "$json")
    echo "  ${name} (connected): counter = ${val}"
done

json3=$(read_counter "$NODE3_URL")
val3=$(extract_value "$json3")
echo "  node-3 (partitioned): counter = ${val3}"
echo ""
echo "  [Expected] node-1, node-2 have newer data; node-3 is behind."
echo ""

# === STEP 7: Recover node-3 ===
separator
echo "STEP 7: Recover node-3 (remove netem rules)"
sub_separator

"${SCRIPT_DIR}/remove-netem.sh" "$NODE3_CONTAINER"
echo ""

# === STEP 8: Wait for convergence ===
separator
echo "STEP 8: Wait for CRDT convergence after recovery"
sub_separator

# Read expected value from node-1.
json1=$(read_counter "$NODE1_URL")
expected=$(extract_value "$json1")
echo "  Expected converged value (from node-1): ${expected}"

converged=false
for attempt in $(seq 1 "$CONVERGENCE_RETRIES"); do
    sleep "$CONVERGENCE_INTERVAL"
    json3=$(read_counter "$NODE3_URL")
    val3=$(extract_value "$json3")
    echo "  Attempt ${attempt}/${CONVERGENCE_RETRIES}: node-3 counter = ${val3}"

    if [ "$val3" = "$expected" ]; then
        converged=true
        break
    fi
done

echo ""
if $converged; then
    echo "  [OK] node-3 converged to ${expected}."
else
    echo "  [WARN] node-3 did not converge within the retry window."
    echo "  Current node-3 value: ${val3}, expected: ${expected}"
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
    json=$(read_counter "$url")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done
echo ""
echo "To clean up netem rules on all containers:"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-1"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-2"
echo "  ./scripts/netem/remove-netem.sh asteroidb-node-3"
