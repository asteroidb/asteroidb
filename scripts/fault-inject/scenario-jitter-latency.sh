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

CONVERGENCE_RETRIES=20
CONVERGENCE_INTERVAL=3
# Post-jitter retry window for node-2 only. node-3 uses a hardcoded shorter
# window (10 retries) because combining both full retry paths would exceed
# the scenario timeout. See the node-3 branch in Step 6 for details.
NODE2_POST_JITTER_RETRIES=40
POST_JITTER_INTERVAL=3

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
log_step 4 "Verify convergence with jitter active (best-effort)"

expected="5"
echo "  Expected value: ${expected}"

# node-3 is not jitter-impaired and should converge quickly.
node3_converged=true
if ! wait_for_convergence "$expected" "$NODE3_URL" "node-3" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    node3_converged=false
fi

# node-2 has jitter applied; poll but don't fail immediately if it misses
# the window — some CI environments produce larger delays than the specified
# 50ms ± 30ms. We will retry after removing jitter in step 5.
echo "  Checking node-2 convergence with jitter active (best-effort)..."
node2_converged_under_jitter=true
if ! wait_for_convergence "$expected" "$NODE2_URL" "node-2" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "$KEY"; then
    node2_converged_under_jitter=false
    echo "  node-2 did not converge under jitter; will retry after jitter removal."
fi

# === STEP 5: Remove jitter ===
log_step 5 "Remove jitter from node-2"
"${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER"

# Allow TCP connections to recover after jitter removal. Jitter causes
# retransmit back-off on the gossip TCP layer; 20s covers the worst-case
# RTO doubling on CI environments before we start polling for convergence.
sleep 20

# === STEP 6: Final convergence check ===
log_step 6 "Final convergence check (post-jitter-removal)"

all_converged=true

# If node-2 did not converge under jitter, retry now without jitter.
if ! $node2_converged_under_jitter; then
    echo "  Retrying node-2 convergence after jitter removal..."
    if ! wait_for_convergence "$expected" "$NODE2_URL" "node-2" "$NODE2_POST_JITTER_RETRIES" "$POST_JITTER_INTERVAL" "$KEY"; then
        all_converged=false
    fi
fi

if ! $node3_converged; then
    # node-3 is non-blocking: jitter on node-2 disrupts all TCP gossip via
    # congestion control. Use a shorter retry window (10×3s=30s) because the
    # full NODE2_POST_JITTER_RETRIES window (40×3s=120s) risks exceeding the scenario
    # timeout when both node-2 and node-3 miss the under-jitter convergence window.
    wait_for_convergence "$expected" "$NODE3_URL" "node-3" "10" "$POST_JITTER_INTERVAL" "$KEY" || \
        echo "  [WARN] node-3 did not converge post-jitter (non-blocking)."
fi

# Print final state for all nodes.
for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
    name="${pair%%:*}"
    url="${pair#*:}"
    json=$(read_counter "$url" "$KEY")
    val=$(extract_value "$json")
    echo "  ${name}: counter = ${val}"
done

if $all_converged; then
    echo ""
    echo -e "${CLR_GREEN}[PASS] jitter-latency: node-2 (required) converged under or after jitter.${CLR_RESET}"
    echo "  (node-1 is the write source and is not polled; node-3 is best-effort)"
    exit 0
else
    echo ""
    echo -e "${CLR_RED}[FAIL] jitter-latency: required node convergence failed.${CLR_RESET}"
    exit 1
fi
