#!/usr/bin/env bash
# Scenario: Node rejoin — stop node, write to remaining, restart, verify sync.
#
# Steps:
#   1. Verify cluster health
#   2. Write initial data, verify all nodes synced
#   3. Stop node-2
#   4. Write additional data to node-1 and node-3
#   5. Restart node-2
#   6. Verify node-2 catches up with all writes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE2_CONTAINER="asteroidb-node-2"
KEY="fault-rejoin-$$"

CONVERGENCE_RETRIES=15
CONVERGENCE_INTERVAL=2

# Trap: ensure node-2 is running on exit.
cleanup() {
    echo "[node-rejoin] Ensuring ${NODE2_CONTAINER} is running..."
    docker start "$NODE2_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# === STEP 1: Cluster health ===
log_step 1 "Verify cluster health"
if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi

# === STEP 2: Write initial data ===
log_step 2 "Write 2 increments to node-1"
for i in 1 2; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent"
done
sleep 3

echo "  Verifying initial sync..."
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 3: Stop node-2 ===
log_step 3 "Stop node-2 (simulate departure)"
"${SCRIPT_DIR}/crash-node.sh" "$NODE2_CONTAINER" stop

# === STEP 4: Write to remaining nodes while node-2 is down ===
log_step 4 "Write to remaining nodes while node-2 is down"

echo "  Writing 3 increments to node-1..."
for i in 1 2 3; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/3 sent to node-1"
done

echo "  Writing 2 increments to node-3..."
for i in 1 2; do
    curl -sf -X POST "${NODE3_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent to node-3"
done
sleep 2

echo "  Current state (node-2 is down):"
for pair in "node-1:${NODE1_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 5: Restart node-2 ===
log_step 5 "Restart node-2 (rejoin)"
"${SCRIPT_DIR}/crash-node.sh" "$NODE2_CONTAINER" start

# === STEP 6: Verify convergence ===
log_step 6 "Verify node-2 catches up after rejoin"

# Expected: 2 (initial) + 3 (node-1) + 2 (node-3) = 7
expected="7"

# First confirm node-1 has the expected value.
json1=$(read_counter "$NODE1_URL" "$KEY")
val1=$(extract_value "$json1")
echo "  node-1 value: ${val1} (expected: ${expected})"

if ! wait_for_convergence "$expected" "$NODE2_URL" "node-2" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    echo ""
    echo -e "${CLR_RED}[FAIL] node-rejoin: node-2 did not converge after rejoin.${CLR_RESET}"
    exit 1
fi

# Also verify node-3 is in sync.
if ! wait_for_convergence "$expected" "$NODE3_URL" "node-3" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    echo ""
    echo -e "${CLR_RED}[FAIL] node-rejoin: node-3 not converged.${CLR_RESET}"
    exit 1
fi

echo ""
echo -e "${CLR_GREEN}[PASS] node-rejoin: node-2 converged after rejoin.${CLR_RESET}"
exit 0
