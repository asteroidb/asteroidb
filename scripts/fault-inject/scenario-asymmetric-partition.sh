#!/usr/bin/env bash
# Scenario: Asymmetric partition — block node-1 -> node-2 but allow node-2 -> node-1.
#
# Steps:
#   1. Verify cluster health
#   2. Create asymmetric partition (node-1 cannot reach node-2)
#   3. Write data to node-1
#   4. Verify node-2 can still receive data via node-3 (indirect path)
#   5. Write data to node-2 and verify node-1 receives it (reverse path works)
#   6. Remove asymmetric partition
#   7. Verify full convergence
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
KEY="fault-asymmetric-$$"

CONVERGENCE_RETRIES=15
CONVERGENCE_INTERVAL=2

# Trap: remove partition on exit.
cleanup() {
    echo "[asymmetric] Cleaning up partition rules..."
    "${SCRIPT_DIR}/remove-asymmetric-partition.sh" "$NODE1_CONTAINER" "$NODE2_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# === STEP 1: Cluster health ===
log_step 1 "Verify cluster health"
if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi

# === STEP 2: Create asymmetric partition ===
log_step 2 "Create asymmetric partition: node-1 -/-> node-2"
"${SCRIPT_DIR}/asymmetric-partition.sh" "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# === STEP 3: Write to node-1 ===
log_step 3 "Write 3 increments to node-1"
for i in 1 2 3; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/3 sent to node-1"
done

# === STEP 4: Check propagation ===
log_step 4 "Check propagation (node-3 should receive, node-2 via indirect path)"
sleep 5

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 5: Write to node-2 (reverse direction should work) ===
log_step 5 "Write 2 increments to node-2 (reverse direction works)"
for i in 1 2; do
    curl -sf -X POST "${NODE2_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent to node-2"
done
sleep 3

for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

# === STEP 6: Remove partition ===
log_step 6 "Remove asymmetric partition"
"${SCRIPT_DIR}/remove-asymmetric-partition.sh" "$NODE1_CONTAINER" "$NODE2_CONTAINER"

# === STEP 7: Verify convergence ===
log_step 7 "Verify full convergence after partition removal"

# Expected total: 3 + 2 = 5
expected="5"
echo "  Expected value: ${expected}"

all_converged=true
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    if ! wait_for_convergence "$expected" "$url" "$name" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
        all_converged=false
    fi
done

if $all_converged; then
    echo ""
    echo -e "${CLR_GREEN}[PASS] asymmetric-partition: all nodes converged.${CLR_RESET}"
    exit 0
else
    echo ""
    echo -e "${CLR_RED}[FAIL] asymmetric-partition: not all nodes converged.${CLR_RESET}"
    exit 1
fi
