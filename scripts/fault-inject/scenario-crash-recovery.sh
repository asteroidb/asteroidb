#!/usr/bin/env bash
# Scenario: Crash recovery — stop a container, wait, restart, verify convergence.
#
# Steps:
#   1. Verify cluster health
#   2. Write data to node-1
#   3. Wait for sync across all nodes
#   4. Stop node-3 (simulate crash)
#   5. Write additional data to node-1 while node-3 is down
#   6. Restart node-3
#   7. Verify node-3 catches up (CRDT convergence)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE3_CONTAINER="asteroidb-node-3"
KEY="fault-crash-recovery-$$"

CONVERGENCE_RETRIES=15
CONVERGENCE_INTERVAL=2

# Trap: ensure node-3 is running on exit.
cleanup() {
    echo "[crash-recovery] Ensuring ${NODE3_CONTAINER} is running..."
    docker start "$NODE3_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# === STEP 1: Cluster health ===
log_step 1 "Verify cluster health"
if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi

# === STEP 2: Write initial data ===
log_step 2 "Write 3 increments to node-1"
for i in 1 2 3; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/3 sent"
done

# === STEP 3: Wait for initial sync ===
log_step 3 "Wait for initial sync"
sleep 3
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 4: Stop node-3 (crash) ===
log_step 4 "Stop node-3 (simulate crash)"
"${SCRIPT_DIR}/crash-node.sh" "$NODE3_CONTAINER" stop

# === STEP 5: Write more data while node-3 is down ===
log_step 5 "Write 5 more increments to node-1 (node-3 is down)"
for i in 1 2 3 4 5; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/5 sent"
done
sleep 2

# Verify node-1 and node-2 have the data.
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 6: Restart node-3 ===
log_step 6 "Restart node-3"
"${SCRIPT_DIR}/crash-node.sh" "$NODE3_CONTAINER" start

# === STEP 7: Verify convergence ===
log_step 7 "Verify CRDT convergence after restart"

json1=$(read_counter "$NODE1_URL" "$KEY")
expected=$(extract_value "$json1")
echo "  Expected value (from node-1): ${expected}"

if wait_for_convergence "$expected" "$NODE3_URL" "node-3" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    echo ""
    echo -e "${CLR_GREEN}[PASS] crash-recovery: node-3 converged after restart.${CLR_RESET}"
    exit 0
else
    echo ""
    echo -e "${CLR_RED}[FAIL] crash-recovery: node-3 did not converge.${CLR_RESET}"
    exit 1
fi
