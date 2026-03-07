#!/usr/bin/env bash
# Multi-node E2E integration test using docker-compose.
#
# Brings up a 3-node AsteroidDB cluster, writes data via one node,
# and verifies convergence by reading from the other nodes.
#
# Usage:
#   ./scripts/e2e-docker.sh
#
# Prerequisites:
#   - docker and docker compose must be available
#   - The project root must contain docker-compose.yml and configs/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

NODES=("localhost:3001" "localhost:3002" "localhost:3003")
NODE_NAMES=("node-1" "node-2" "node-3")

# Maximum seconds to wait for a node to become healthy.
HEALTH_TIMEOUT=120
# Seconds between polls.
POLL_INTERVAL=2

# Track whether we should clean up on exit.
CLEANUP=true

cleanup() {
    if $CLEANUP; then
        echo ""
        echo "==> Cleaning up: stopping cluster..."
        docker compose -f "$COMPOSE_FILE" down --timeout 10 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------
# Step 1: Bring up the cluster
# ---------------------------------------------------------------
echo "==> Building and starting 3-node cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build

# ---------------------------------------------------------------
# Step 2: Wait for all nodes to become healthy
# ---------------------------------------------------------------
echo "==> Waiting for nodes to become healthy (timeout: ${HEALTH_TIMEOUT}s)..."

wait_for_node() {
    local addr="$1"
    local name="$2"
    local elapsed=0

    while [ "$elapsed" -lt "$HEALTH_TIMEOUT" ]; do
        # Use the eventual read endpoint as a health check (returns 200 even for
        # nonexistent keys).
        if curl -sf --max-time 3 "http://${addr}/api/eventual/__health" > /dev/null 2>&1; then
            echo "  ${name} (${addr}): UP"
            return 0
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    echo "  ${name} (${addr}): TIMEOUT after ${HEALTH_TIMEOUT}s"
    return 1
}

all_healthy=true
for i in "${!NODES[@]}"; do
    if ! wait_for_node "${NODES[$i]}" "${NODE_NAMES[$i]}"; then
        all_healthy=false
    fi
done

if ! $all_healthy; then
    echo "ERROR: Not all nodes became healthy. Aborting."
    echo ""
    echo "Docker compose logs:"
    docker compose -f "$COMPOSE_FILE" logs --tail=50
    exit 1
fi

echo ""
echo "All nodes healthy."

# ---------------------------------------------------------------
# Step 3: Write data via node-1
# ---------------------------------------------------------------
echo ""
echo "==> Writing key 'e2e-test' = 'hello-from-docker' to node-1..."

write_resp=$(curl -sf -X POST "http://${NODES[0]}/api/eventual/write" \
    -H "Content-Type: application/json" \
    -d '{"type":"register_set","key":"e2e-test","value":"hello-from-docker"}')

echo "  Write response: ${write_resp:-OK}"

# Give the cluster a moment for anti-entropy sync to propagate.
echo "  Waiting 5s for sync propagation..."
sleep 5

# ---------------------------------------------------------------
# Step 4: Read from all nodes and verify convergence
# ---------------------------------------------------------------
echo ""
echo "==> Verifying convergence across all nodes..."

failures=0

for i in "${!NODES[@]}"; do
    addr="${NODES[$i]}"
    name="${NODE_NAMES[$i]}"

    resp=$(curl -sf "http://${addr}/api/eventual/e2e-test" 2>&1 || true)

    if [ -z "$resp" ]; then
        echo "  ${name}: FAIL (no response)"
        failures=$((failures + 1))
        continue
    fi

    # Check that the response contains the expected value.
    # Using grep for basic validation (no jq dependency required).
    if echo "$resp" | grep -q '"hello-from-docker"'; then
        echo "  ${name}: OK (value = hello-from-docker)"
    else
        echo "  ${name}: FAIL (unexpected response: ${resp})"
        failures=$((failures + 1))
    fi
done

# ---------------------------------------------------------------
# Step 5: Test metrics endpoint on each node
# ---------------------------------------------------------------
echo ""
echo "==> Checking /api/metrics on all nodes..."

for i in "${!NODES[@]}"; do
    addr="${NODES[$i]}"
    name="${NODE_NAMES[$i]}"

    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "http://${addr}/api/metrics" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "  ${name}: OK (HTTP 200)"
    else
        echo "  ${name}: FAIL (HTTP ${status})"
        failures=$((failures + 1))
    fi
done

# ---------------------------------------------------------------
# Step 6: Cross-node write/read (write to node-2, read from node-3)
# ---------------------------------------------------------------
echo ""
echo "==> Cross-node test: write to node-2, read from node-3..."

curl -sf -X POST "http://${NODES[1]}/api/eventual/write" \
    -H "Content-Type: application/json" \
    -d '{"type":"register_set","key":"cross-node","value":"from-node-2"}' > /dev/null

sleep 5

resp=$(curl -sf "http://${NODES[2]}/api/eventual/cross-node" 2>&1 || true)
if echo "$resp" | grep -q '"from-node-2"'; then
    echo "  node-3 read: OK (converged from node-2)"
else
    echo "  node-3 read: FAIL (response: ${resp})"
    failures=$((failures + 1))
fi

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "==============================="
if [ "$failures" -eq 0 ]; then
    echo "E2E RESULT: ALL TESTS PASSED"
    echo "==============================="
    exit 0
else
    echo "E2E RESULT: ${failures} FAILURE(S)"
    echo "==============================="
    exit 1
fi
