#!/usr/bin/env bash
# Scenario: Rolling partition — sequentially isolate each node, then heal all.
#
# Steps:
#   1. Verify cluster health
#   2. Write initial data
#   3. Partition node-1, write to node-2, heal node-1
#   4. Partition node-2, write to node-3, heal node-2
#   5. Partition node-3, write to node-1, heal node-3
#   6. Verify all nodes converge to the same state
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"
KEY="fault-rolling-$$"

CONVERGENCE_RETRIES=20
CONVERGENCE_INTERVAL=2

# Trap: remove all netem rules on exit.
cleanup() {
    echo "[rolling] Cleaning up netem rules on all nodes..."
    "${NETEM_DIR}/remove-netem.sh" "$NODE1_CONTAINER" 2>/dev/null || true
    "${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER" 2>/dev/null || true
    "${NETEM_DIR}/remove-netem.sh" "$NODE3_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# === STEP 1: Cluster health ===
log_step 1 "Verify cluster health"
if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi

# === STEP 2: Write initial data ===
log_step 2 "Write 2 initial increments to node-1"
for i in 1 2; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent"
done
sleep 3

# === STEP 3: Partition node-1, write to node-2, heal ===
log_step 3 "Partition node-1, write 2 to node-2, heal node-1"
"${NETEM_DIR}/add-partition.sh" "$NODE1_CONTAINER"

for i in 1 2; do
    curl -sf -X POST "${NODE2_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent to node-2"
done
sleep 3

"${NETEM_DIR}/remove-netem.sh" "$NODE1_CONTAINER"
sleep 3

# === STEP 4: Partition node-2, write to node-3, heal ===
log_step 4 "Partition node-2, write 2 to node-3, heal node-2"
"${NETEM_DIR}/add-partition.sh" "$NODE2_CONTAINER"

for i in 1 2; do
    curl -sf -X POST "${NODE3_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent to node-3"
done
sleep 3

"${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER"
sleep 3

# === STEP 5: Partition node-3, write to node-1, heal ===
log_step 5 "Partition node-3, write 2 to node-1, heal node-3"
"${NETEM_DIR}/add-partition.sh" "$NODE3_CONTAINER"

for i in 1 2; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/2 sent to node-1"
done
sleep 3

"${NETEM_DIR}/remove-netem.sh" "$NODE3_CONTAINER"

# === STEP 6: Verify convergence ===
log_step 6 "Verify full convergence (expected total: 8)"

# Total: 2 (initial) + 2 (step3) + 2 (step4) + 2 (step5) = 8
expected="8"
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
    echo -e "${CLR_GREEN}[PASS] rolling-partition: all nodes converged after rolling partitions.${CLR_RESET}"
    exit 0
else
    echo ""
    echo -e "${CLR_RED}[FAIL] rolling-partition: not all nodes converged.${CLR_RESET}"
    exit 1
fi
