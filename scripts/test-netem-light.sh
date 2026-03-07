#!/usr/bin/env bash
# Lightweight netem test scenarios for PR CI.
#
# Runs three quick network-fault scenarios against a 3-node Docker cluster:
#   1. Delay:        100ms added to node-2, write on node-1, verify convergence
#   2. Packet loss:  5% loss on node-2, write on node-1, verify convergence
#   3. Partition:    node-3 fully partitioned for 3s, recover, verify convergence
#
# Each scenario is wrapped in a function with its own trap to guarantee netem
# rules are cleaned up even if the scenario fails mid-way (set -e).
#
# Usage: ./scripts/test-netem-light.sh
#
# Prerequisites:
#   - Docker and docker compose available
#   - python3 available on the host (used by lib.sh for JSON parsing)
#   - No other asteroidb containers running (ports 3001-3003 free)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/netem"
source "${NETEM_DIR}/lib.sh"

# --- Configuration ---
NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"

COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"

PASS_COUNT=0
FAIL_COUNT=0
TOTAL_START=$(date +%s)

# --- Helper functions ---

cleanup() {
    echo ""
    echo "[light-netem] Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

wait_for_cluster() {
    echo "[light-netem] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 20); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 20 ]; then
                echo "[light-netem] ERROR: Node on port ${port} did not become ready"
                exit 1
            fi
            sleep 1
        done
    done
    echo "[light-netem] Cluster is ready."
}

write_counter() {
    local url="$1"
    local key="$2"
    local count="${3:-1}"
    for _ in $(seq 1 "$count"); do
        curl -sf -X POST "${url}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" > /dev/null
    done
}

check_convergence() {
    local expected="$1"
    local key="$2"
    shift 2
    # remaining args are "name:url" pairs
    local retries=10
    local interval=1

    for pair in "$@"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local converged=false

        for attempt in $(seq 1 "$retries"); do
            local json val
            json=$(read_counter "$url" "$key")
            val=$(extract_value "$json")
            if [ "$val" = "$expected" ]; then
                converged=true
                break
            fi
            sleep "$interval"
        done

        if $converged; then
            echo -e "  ${CLR_GREEN}[OK] ${name} converged to ${expected}${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name} did not converge (got ${val}, expected ${expected})${CLR_RESET}"
            return 1
        fi
    done
    return 0
}

scenario_result() {
    local name="$1"
    local exit_code="$2"
    local start_time="$3"
    local end_time
    end_time=$(date +%s)
    local duration=$(( end_time - start_time ))

    if [ "$exit_code" -eq 0 ]; then
        echo -e "${CLR_GREEN}[PASS] ${name} (${duration}s)${CLR_RESET}"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        echo -e "${CLR_RED}[FAIL] ${name} (${duration}s)${CLR_RESET}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi
}

# --- Scenario functions ---
# Each scenario is a function that returns 0 on success, 1 on failure.
# Netem cleanup is guaranteed by a local trap so that set -e mid-scenario
# failures do not leave tc rules behind.

run_scenario_delay() {
    local key="netem-light-delay-$$"
    local exit_code=0

    # Ensure netem is removed even on early exit
    trap '"${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER" 2>/dev/null || true' RETURN

    # Add 100ms delay
    "${NETEM_DIR}/add-delay.sh" "$NODE2_CONTAINER" 100

    # Write 3 increments to node-1
    echo "[scenario] Writing 3 increments to node-1..."
    write_counter "$NODE1_URL" "$key" 3

    # Check convergence on node-2 and node-3
    echo "[scenario] Checking convergence..."
    if ! check_convergence "3" "$key" \
        "node-2:${NODE2_URL}" \
        "node-3:${NODE3_URL}"; then
        exit_code=1
    fi

    return "$exit_code"
}

run_scenario_loss() {
    local key="netem-light-loss-$$"
    local exit_code=0

    # Ensure netem is removed even on early exit
    trap '"${NETEM_DIR}/remove-netem.sh" "$NODE2_CONTAINER" 2>/dev/null || true' RETURN

    # Add 5% packet loss
    echo "[netem] Adding 5% packet loss to ${NODE2_CONTAINER}..."
    docker exec "$NODE2_CONTAINER" tc qdisc del dev eth0 root 2>/dev/null || true
    docker exec "$NODE2_CONTAINER" tc qdisc add dev eth0 root netem loss 5%
    echo "[netem] ${NODE2_CONTAINER}: 5% packet loss applied."

    # Write 3 increments to node-1
    echo "[scenario] Writing 3 increments to node-1..."
    write_counter "$NODE1_URL" "$key" 3

    # Check convergence on node-2 and node-3
    echo "[scenario] Checking convergence..."
    if ! check_convergence "3" "$key" \
        "node-2:${NODE2_URL}" \
        "node-3:${NODE3_URL}"; then
        exit_code=1
    fi

    return "$exit_code"
}

run_scenario_partition() {
    local key="netem-light-partition-$$"
    local exit_code=0

    # Ensure netem is removed even on early exit
    trap '"${NETEM_DIR}/remove-netem.sh" "$NODE3_CONTAINER" 2>/dev/null || true' RETURN

    # Write initial data so all nodes have baseline
    echo "[scenario] Writing 2 increments to node-1 (baseline)..."
    write_counter "$NODE1_URL" "$key" 2
    sleep 2

    # Partition node-3
    echo "[scenario] Partitioning node-3..."
    "${NETEM_DIR}/add-partition.sh" "$NODE3_CONTAINER"

    # Write 3 more increments while node-3 is partitioned
    echo "[scenario] Writing 3 increments during partition..."
    write_counter "$NODE1_URL" "$key" 3

    # Hold partition for 3 seconds (longer than sync interval of 2s)
    sleep 3

    # Recover
    echo "[scenario] Recovering node-3..."
    "${NETEM_DIR}/remove-netem.sh" "$NODE3_CONTAINER"

    # Verify convergence: total should be 5
    echo "[scenario] Checking convergence after recovery..."
    if ! check_convergence "5" "$key" \
        "node-1:${NODE1_URL}" \
        "node-2:${NODE2_URL}" \
        "node-3:${NODE3_URL}"; then
        exit_code=1
    fi

    return "$exit_code"
}

# --- Start cluster ---
separator
echo -e "${CLR_BOLD}AsteroidDB Lightweight Netem Tests${CLR_RESET}"
separator
echo ""

echo "[light-netem] Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
wait_for_cluster
echo ""

# ======================================================================
# Scenario 1: Delay (100ms on node-2)
# ======================================================================
separator
echo -e "${CLR_BOLD}Scenario 1/3: Delay (100ms on node-2)${CLR_RESET}"
sub_separator

S1_START=$(date +%s)
S1_EXIT=0
run_scenario_delay || S1_EXIT=$?
scenario_result "Delay (100ms)" "$S1_EXIT" "$S1_START"
echo ""

# ======================================================================
# Scenario 2: Packet Loss (5% on node-2)
# ======================================================================
separator
echo -e "${CLR_BOLD}Scenario 2/3: Packet Loss (5% on node-2)${CLR_RESET}"
sub_separator

S2_START=$(date +%s)
S2_EXIT=0
run_scenario_loss || S2_EXIT=$?
scenario_result "Packet Loss (5%)" "$S2_EXIT" "$S2_START"
echo ""

# ======================================================================
# Scenario 3: Partition (node-3 isolated for 3s, then recover)
# ======================================================================
separator
echo -e "${CLR_BOLD}Scenario 3/3: Partition (node-3 for 3s)${CLR_RESET}"
sub_separator

S3_START=$(date +%s)
S3_EXIT=0
run_scenario_partition || S3_EXIT=$?
scenario_result "Partition (3s)" "$S3_EXIT" "$S3_START"
echo ""

# ======================================================================
# Summary
# ======================================================================
TOTAL_END=$(date +%s)
TOTAL_DURATION=$(( TOTAL_END - TOTAL_START ))

separator
echo -e "${CLR_BOLD}Summary${CLR_RESET}"
sub_separator
echo "  Passed: ${PASS_COUNT}"
echo "  Failed: ${FAIL_COUNT}"
echo "  Total time: ${TOTAL_DURATION}s"
separator

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${CLR_RED}Some scenarios failed.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}All scenarios passed.${CLR_RESET}"
exit 0
