#!/usr/bin/env bash
# Scenario: Jitter latency — add 50ms +/- 30ms jitter, verify operations complete.
#
# Steps:
#   1. Verify cluster health
#   2. Add 50ms +/- 30ms jitter to node-2
#   3. Write data to node-1
#   4. Verify all nodes converge despite jitter
#   5. Remove jitter
#   6. Verify final convergence
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE2_CONTAINER="asteroidb-node-2"
KEY="fault-jitter-$$"

CONVERGENCE_RETRIES=15
CONVERGENCE_INTERVAL=2

# Trap: remove netem on exit.
cleanup() {
    echo "[jitter] Cleaning up netem rules..."
    "${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER" 2>/dev/null || true
}
trap cleanup EXIT

# === STEP 1: Cluster health ===
log_step 1 "Verify cluster health"
if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
    exit 1
fi

# === STEP 2: Add jitter ===
log_step 2 "Add 50ms +/- 30ms jitter to node-2"

# Ensure tc is available.
if ! docker exec "$NODE2_CONTAINER" which tc > /dev/null 2>&1; then
    echo "[jitter] tc not found in ${NODE2_CONTAINER}, installing iproute2..."
    docker exec "$NODE2_CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq iproute2 > /dev/null 2>&1"
fi
docker exec "$NODE2_CONTAINER" tc qdisc del dev eth0 root 2>/dev/null || true
docker exec "$NODE2_CONTAINER" tc qdisc add dev eth0 root netem delay 50ms 30ms distribution normal
echo "[jitter] node-2: 50ms +/- 30ms jitter applied."

# === STEP 3: Write data ===
log_step 3 "Write 5 increments to node-1"
for i in 1 2 3 4 5; do
    curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${KEY}\"}" > /dev/null
    echo "  Increment ${i}/5 sent"
done

# === STEP 4: Verify convergence with jitter active ===
log_step 4 "Verify convergence with jitter active"

expected="5"
echo "  Expected value: ${expected}"

all_converged=true
for pair in "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    if ! wait_for_convergence "$expected" "$url" "$name" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
        all_converged=false
    fi
done

# === STEP 5: Remove jitter ===
log_step 5 "Remove jitter from node-2"
"${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER"

# === STEP 6: Final state ===
log_step 6 "Final state"
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

if $all_converged; then
    echo ""
    echo -e "${CLR_GREEN}[PASS] jitter-latency: all nodes converged despite jitter.${CLR_RESET}"
    exit 0
else
    echo ""
    echo -e "${CLR_RED}[FAIL] jitter-latency: not all nodes converged.${CLR_RESET}"
    exit 1
fi
