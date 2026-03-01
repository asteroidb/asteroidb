#!/usr/bin/env bash
#
# E2E test: verify that eventual writes replicate across the 3-node
# docker-compose cluster via anti-entropy sync.
#
# Usage:
#   ./scripts/test-docker-replication.sh
#
# Prerequisites:
#   - Docker and Docker Compose V2 installed
#   - Ports 3001-3003 available

set -euo pipefail

NODE1="http://localhost:3001"
NODE2="http://localhost:3002"
NODE3="http://localhost:3003"
TIMEOUT_SECS=30
POLL_INTERVAL=1

cleanup() {
    echo "==> Stopping cluster..."
    docker compose down --timeout 5 2>/dev/null || true
}

fail() {
    echo "FAIL: $1" >&2
    cleanup
    exit 1
}

trap cleanup EXIT

echo "==> Building and starting 3-node cluster..."
docker compose up -d --build --wait 2>&1 || fail "docker compose up failed"

# Wait for all nodes to be reachable
echo "==> Waiting for nodes to become ready..."
for port in 3001 3002 3003; do
    for i in $(seq 1 "$TIMEOUT_SECS"); do
        if curl -sf "http://localhost:${port}/api/eventual/healthcheck" >/dev/null 2>&1 || \
           curl -sf -o /dev/null -w '%{http_code}' "http://localhost:${port}/api/eventual/healthcheck" 2>/dev/null | grep -qE '(200|404)'; then
            break
        fi
        if [ "$i" -eq "$TIMEOUT_SECS" ]; then
            fail "node on port ${port} did not become ready within ${TIMEOUT_SECS}s"
        fi
        sleep "$POLL_INTERVAL"
    done
done
echo "    All nodes ready."

# --- Test 1: Counter replication ---
echo ""
echo "==> Test 1: Counter replication (node-1 -> node-2, node-3)"

curl -sf -X POST "${NODE1}/api/eventual/write" \
    -H "Content-Type: application/json" \
    -d '{"type":"counter_inc","key":"test-counter"}' >/dev/null \
    || fail "counter_inc write to node-1 failed"

echo "    Wrote counter_inc to node-1. Polling node-2 and node-3..."

for node_label in "node-2:${NODE2}" "node-3:${NODE3}"; do
    label="${node_label%%:*}"
    url="${node_label#*:}"

    converged=false
    for i in $(seq 1 "$TIMEOUT_SECS"); do
        result=$(curl -sf "${url}/api/eventual/test-counter" 2>/dev/null || echo "")
        if echo "$result" | grep -q '"value":1'; then
            echo "    ${label}: converged (attempt ${i})"
            converged=true
            break
        fi
        sleep "$POLL_INTERVAL"
    done

    if [ "$converged" != "true" ]; then
        fail "${label} did not converge within ${TIMEOUT_SECS}s. Last response: ${result}"
    fi
done

echo "    PASS: Counter replicated to all nodes."

# --- Test 2: Set replication ---
echo ""
echo "==> Test 2: Set replication (node-2 -> node-1, node-3)"

curl -sf -X POST "${NODE2}/api/eventual/write" \
    -H "Content-Type: application/json" \
    -d '{"type":"set_add","key":"test-set","element":"alice"}' >/dev/null \
    || fail "set_add write to node-2 failed"

echo "    Wrote set_add to node-2. Polling node-1 and node-3..."

for node_label in "node-1:${NODE1}" "node-3:${NODE3}"; do
    label="${node_label%%:*}"
    url="${node_label#*:}"

    converged=false
    for i in $(seq 1 "$TIMEOUT_SECS"); do
        result=$(curl -sf "${url}/api/eventual/test-set" 2>/dev/null || echo "")
        if echo "$result" | grep -q 'alice'; then
            echo "    ${label}: converged (attempt ${i})"
            converged=true
            break
        fi
        sleep "$POLL_INTERVAL"
    done

    if [ "$converged" != "true" ]; then
        fail "${label} did not converge within ${TIMEOUT_SECS}s. Last response: ${result}"
    fi
done

echo "    PASS: Set replicated to all nodes."

# --- Test 3: Register replication ---
echo ""
echo "==> Test 3: Register replication (node-3 -> node-1, node-2)"

curl -sf -X POST "${NODE3}/api/eventual/write" \
    -H "Content-Type: application/json" \
    -d '{"type":"register_set","key":"test-register","value":"hello-world"}' >/dev/null \
    || fail "register_set write to node-3 failed"

echo "    Wrote register_set to node-3. Polling node-1 and node-2..."

for node_label in "node-1:${NODE1}" "node-2:${NODE2}"; do
    label="${node_label%%:*}"
    url="${node_label#*:}"

    converged=false
    for i in $(seq 1 "$TIMEOUT_SECS"); do
        result=$(curl -sf "${url}/api/eventual/test-register" 2>/dev/null || echo "")
        if echo "$result" | grep -q 'hello-world'; then
            echo "    ${label}: converged (attempt ${i})"
            converged=true
            break
        fi
        sleep "$POLL_INTERVAL"
    done

    if [ "$converged" != "true" ]; then
        fail "${label} did not converge within ${TIMEOUT_SECS}s. Last response: ${result}"
    fi
done

echo "    PASS: Register replicated to all nodes."

echo ""
echo "==============================="
echo " All replication tests passed!"
echo "==============================="
