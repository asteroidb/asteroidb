#!/usr/bin/env bash
# Scenario: High latency impact on CRDT convergence time.
#
# This script measures how network delay affects CRDT convergence:
#   1. Verify the 3-node cluster is up
#   2. Add 200ms delay to node-2
#   3. Write counter increments to node-1
#   4. Measure time for values to appear on node-2 (delayed) and node-3 (normal)
#   5. Remove delay
#   6. Verify all nodes converge
#   7. Report timing results
#
# Usage: ./scripts/netem/scenario-delay-convergence.sh
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
NODE2_CONTAINER="asteroidb-node-2"
KEY="netem-delay-test-key"
DELAY_MS=200

# Interval (seconds) to wait for data propagation.
SYNC_WAIT=3
# Additional wait to account for delay.
DELAY_SYNC_WAIT=5
# Maximum retries when waiting for convergence.
CONVERGENCE_RETRIES=10
CONVERGENCE_INTERVAL=2

# --- STEP 1: Cluster health check ---
log_step 1 "Verify cluster health"

if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi
echo ""

# --- STEP 2: Add 200ms delay to node-2 ---
log_step 2 "Add ${DELAY_MS}ms delay to node-2"

"${SCRIPT_DIR}/add-delay.sh" "$NODE2_CONTAINER" "$DELAY_MS"
echo ""

# --- STEP 3: Write counter increments to node-1 ---
log_step 3 "Write counter to node-1 (5 increments)"

for i in 1 2 3 4 5; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/5 sent to node-1"
done
echo ""

# --- STEP 4: Measure convergence time on node-3 (no delay) ---
log_step 4 "Measure convergence time on node-3 (no delay)"

start_ms=$(now_epoch_ms)
node3_converged=false
node3_elapsed="N/A"

for attempt in $(seq 1 "$CONVERGENCE_RETRIES"); do
    sleep "$CONVERGENCE_INTERVAL"
    json=$(read_counter "$NODE3_URL" "$KEY")
    val=$(extract_value "$json")
    echo "  Attempt ${attempt}/${CONVERGENCE_RETRIES}: node-3 counter = ${val}"

    if [ "$val" = "5" ]; then
        node3_elapsed=$(elapsed_ms "$start_ms")
        node3_converged=true
        echo -e "  ${CLR_GREEN}[OK] node-3 converged in ${node3_elapsed}ms${CLR_RESET}"
        break
    fi
done

if ! $node3_converged; then
    node3_elapsed=$(elapsed_ms "$start_ms")
    echo -e "  ${CLR_YELLOW}[WARN] node-3 did not converge within retry window (${node3_elapsed}ms elapsed)${CLR_RESET}"
fi
echo ""

# --- STEP 5: Measure convergence time on node-2 (200ms delay) ---
log_step 5 "Measure convergence time on node-2 (${DELAY_MS}ms delay)"

start_ms=$(now_epoch_ms)
node2_converged=false
node2_elapsed="N/A"

for attempt in $(seq 1 "$CONVERGENCE_RETRIES"); do
    sleep "$CONVERGENCE_INTERVAL"
    json=$(read_counter "$NODE2_URL" "$KEY")
    val=$(extract_value "$json")
    echo "  Attempt ${attempt}/${CONVERGENCE_RETRIES}: node-2 counter = ${val}"

    if [ "$val" = "5" ]; then
        node2_elapsed=$(elapsed_ms "$start_ms")
        node2_converged=true
        echo -e "  ${CLR_GREEN}[OK] node-2 converged in ${node2_elapsed}ms${CLR_RESET}"
        break
    fi
done

if ! $node2_converged; then
    node2_elapsed=$(elapsed_ms "$start_ms")
    echo -e "  ${CLR_YELLOW}[WARN] node-2 did not converge within retry window (${node2_elapsed}ms elapsed)${CLR_RESET}"
fi
echo ""

# --- STEP 6: Remove delay from node-2 ---
log_step 6 "Remove delay from node-2"

"${SCRIPT_DIR}/remove-netem.sh" "$NODE2_CONTAINER"
echo ""

# --- STEP 7: Verify final convergence ---
log_step 7 "Verify final convergence (all nodes)"

sleep "$SYNC_WAIT"

all_converged=true
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
    if [ "$val" != "5" ]; then
        all_converged=false
    fi
done
echo ""

# --- STEP 8: Report timing results ---
log_step 8 "Timing results"

echo ""
echo "  Delay applied to node-2:  ${DELAY_MS}ms"
echo "  node-3 (no delay):        ${node3_elapsed}ms to converge"
echo "  node-2 (${DELAY_MS}ms delay):   ${node2_elapsed}ms to converge"
echo ""

if $node3_converged && $node2_converged; then
    echo -e "  ${CLR_GREEN}[RESULT] Both nodes converged.${CLR_RESET}"
elif $node3_converged; then
    echo -e "  ${CLR_YELLOW}[RESULT] node-3 converged but node-2 (delayed) did not.${CLR_RESET}"
else
    echo -e "  ${CLR_YELLOW}[RESULT] Neither node converged within the retry window.${CLR_RESET}"
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
