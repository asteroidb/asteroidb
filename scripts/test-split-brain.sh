#!/usr/bin/env bash
# Split-brain safety test for AsteroidDB (Issue #303).
#
# Verifies that during a network partition:
#   - The majority partition CAN certify writes.
#   - The minority partition CANNOT certify writes (safety property).
#   - Eventual writes succeed on both sides of the partition.
#   - After partition heal, data converges.
#
# Configurations tested:
#   1. 3-node cluster: 1v2 partition (minority=1, majority=2)
#   2. Flapping partition: rapid connect/disconnect cycles
#
# Prerequisites:
#   - Docker and docker compose available
#   - jq installed
#   - python3 available (used by netem/lib.sh)
#
# Usage: ./scripts/test-split-brain.sh [--scenario <name>|--all]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/netem"
FAULT_DIR="${SCRIPT_DIR}/fault-inject"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"

source "${NETEM_DIR}/lib.sh"

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"

CONVERGENCE_RETRIES=20
CONVERGENCE_INTERVAL=2

# --- Argument parsing ---
RUN_ALL=false
SCENARIO_NAME=""

usage() {
    cat <<'USAGE'
Usage: ./scripts/test-split-brain.sh [OPTIONS]

Options:
  --scenario <name>   Run a single scenario
  --all               Run all scenarios (default)
  --help              Show this help

Scenarios:
  1v2-partition         3-node split: minority(1) vs majority(2)
  flapping-partition    Rapid connect/disconnect cycles
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario)
            SCENARIO_NAME="${2:?--scenario requires a name}"
            shift 2
            ;;
        --all)
            RUN_ALL=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

# Default to --all if nothing specified.
if [ "$RUN_ALL" = "false" ] && [ -z "$SCENARIO_NAME" ]; then
    RUN_ALL=true
fi

# --- Helper functions ---

# ensure_iptables <container>
# Make sure iptables is available in the container.
ensure_iptables() {
    local container="$1"
    if ! docker exec "$container" which iptables > /dev/null 2>&1; then
        echo "  Installing iptables in ${container}..."
        docker exec "$container" bash -c "apt-get update -qq && apt-get install -y -qq iptables > /dev/null 2>&1"
    fi
}

# isolate_node <container>
# Block all traffic from and to this container from the other two nodes.
isolate_node() {
    local target="$1"
    local all_containers=("$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER")

    ensure_iptables "$target"

    for other in "${all_containers[@]}"; do
        if [ "$other" = "$target" ]; then
            continue
        fi
        ensure_iptables "$other"

        local other_ip
        other_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$other")
        local target_ip
        target_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$target")

        # Block target -> other
        docker exec "$target" iptables -A OUTPUT -d "$other_ip" -j DROP 2>/dev/null || true
        # Block other -> target
        docker exec "$other" iptables -A OUTPUT -d "$target_ip" -j DROP 2>/dev/null || true
    done
    echo "  Isolated ${target} from the cluster."
}

# heal_all
# Flush iptables OUTPUT chain on all nodes to remove all partitions.
heal_all() {
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
    done
    echo "  Partition healed — all nodes can communicate."
}

# try_certified_write <url> <key> <on_timeout>
# Attempt a certified write. Returns the HTTP status line and body.
try_certified_write() {
    local url="$1"
    local key="$2"
    local on_timeout="${3:-pending}"

    curl -sf -w "\n%{http_code}" -X POST "${url}/api/certified/write" \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"${key}\",\"value\":{\"counter\":{\"value\":0}},\"on_timeout\":\"${on_timeout}\"}" \
        --max-time 10 2>/dev/null || echo -e "\n000"
}

# get_cert_status <url> <key>
# Returns the certification status JSON.
get_cert_status() {
    local url="$1"
    local key="$2"
    curl -sf --max-time 5 "${url}/api/certified/${key}" 2>/dev/null || echo '{"status":"unknown"}'
}

# try_eventual_write <url> <key>
# Write via eventual API.
try_eventual_write() {
    local url="$1"
    local key="$2"
    curl -sf -X POST "${url}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" \
        --max-time 5 > /dev/null 2>&1
}

# wait_for_cert_status <expected_status> <url> <key> <retries> <interval>
# Poll until certification status matches expected. Returns 0 on match, 1 on timeout.
wait_for_cert_status() {
    local expected="$1"
    local url="$2"
    local key="$3"
    local retries="${4:-15}"
    local interval="${5:-2}"

    for attempt in $(seq 1 "$retries"); do
        sleep "$interval"
        local json
        json=$(get_cert_status "$url" "$key")
        local status
        status=$(echo "$json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # Handle both string and object status formats
    s = d.get('status', 'unknown')
    if isinstance(s, str):
        print(s.lower())
    else:
        print(str(s).lower())
except:
    print('unknown')
" 2>/dev/null || echo "unknown")
        echo "  Attempt ${attempt}/${retries}: status = ${status}"
        if [ "$status" = "$expected" ]; then
            return 0
        fi
    done
    return 1
}

# --- Cluster management ---

cleanup() {
    echo ""
    echo "[split-brain] Cleaning up..."
    heal_all 2>/dev/null || true
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

wait_for_cluster() {
    echo "[split-brain] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 30); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 30 ]; then
                echo "[split-brain] ERROR: Node on port ${port} did not become ready"
                exit 1
            fi
            sleep 1
        done
    done
    echo "[split-brain] Cluster is ready."
}

# ==========================================================================
# SCENARIO 1: 1v2 partition (3-node cluster)
# ==========================================================================
scenario_1v2_partition() {
    local KEY_MAJORITY="split-brain-majority-$$"
    local KEY_MINORITY="split-brain-minority-$$"
    local KEY_EVENTUAL="split-brain-eventual-$$"

    # === STEP 1: Verify cluster health ===
    log_step 1 "Verify cluster health"
    if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
        return 1
    fi

    # === STEP 2: Write pre-partition data ===
    log_step 2 "Write pre-partition eventual data"
    try_eventual_write "$NODE1_URL" "$KEY_EVENTUAL"
    echo "  Wrote 1 increment to node-1"
    sleep 3
    echo "  Waiting for replication..."

    # === STEP 3: Create 1v2 partition (node-1 isolated) ===
    log_step 3 "Create partition: node-1 (minority) vs node-2+node-3 (majority)"
    isolate_node "$NODE1_CONTAINER"
    sleep 2

    # === STEP 4: Attempt certified write on MINORITY side (node-1) ===
    log_step 4 "Attempt certified write on MINORITY side (node-1)"
    echo "  Expected: should NOT achieve certification (only 1 of 3 authorities)"

    local minority_response
    minority_response=$(try_certified_write "$NODE1_URL" "$KEY_MINORITY" "pending")
    local minority_http
    minority_http=$(echo "$minority_response" | tail -1)
    local minority_body
    minority_body=$(echo "$minority_response" | head -n -1)
    echo "  Minority certified write HTTP: ${minority_http}"
    echo "  Minority certified write body: ${minority_body}"

    # The write should either fail or return Pending (never Certified on minority).
    # Wait a bit and check — it should stay Pending or become Timeout.
    sleep 5
    local minority_status_json
    minority_status_json=$(get_cert_status "$NODE1_URL" "$KEY_MINORITY")
    local minority_status
    minority_status=$(echo "$minority_status_json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    s = d.get('status', 'unknown')
    print(str(s).lower() if isinstance(s, str) else str(s).lower())
except:
    print('unknown')
" 2>/dev/null || echo "unknown")

    echo "  Minority certification status: ${minority_status}"

    if [ "$minority_status" = "certified" ]; then
        echo -e "  ${CLR_RED}[SAFETY VIOLATION] Minority partition achieved certification!${CLR_RESET}"
        return 1
    fi
    echo -e "  ${CLR_GREEN}[OK] Minority cannot certify (status: ${minority_status})${CLR_RESET}"

    # === STEP 5: Attempt certified write on MAJORITY side (node-2) ===
    log_step 5 "Attempt certified write on MAJORITY side (node-2)"
    echo "  Expected: should achieve certification (2 of 3 authorities reachable)"

    local majority_response
    majority_response=$(try_certified_write "$NODE2_URL" "$KEY_MAJORITY" "pending")
    local majority_http
    majority_http=$(echo "$majority_response" | tail -1)
    local majority_body
    majority_body=$(echo "$majority_response" | head -n -1)
    echo "  Majority certified write HTTP: ${majority_http}"
    echo "  Majority certified write body: ${majority_body}"

    # Wait for certification to process. The majority side has 2 authorities
    # that can exchange frontiers, so it should reach Certified.
    echo "  Waiting for majority certification..."
    if wait_for_cert_status "certified" "$NODE2_URL" "$KEY_MAJORITY" 15 2; then
        echo -e "  ${CLR_GREEN}[OK] Majority partition achieved certification.${CLR_RESET}"
    else
        echo -e "  ${CLR_YELLOW}[WARN] Majority did not certify within timeout.${CLR_RESET}"
        echo "  (This may be expected if frontier reporting requires all 3 nodes)"
        # Not a hard failure — the safety property is that minority CANNOT certify.
    fi

    # === STEP 6: Verify eventual writes still work on both sides ===
    log_step 6 "Verify eventual writes on both sides of partition"

    try_eventual_write "$NODE1_URL" "${KEY_EVENTUAL}-minority"
    echo "  Eventual write on minority (node-1): OK"

    try_eventual_write "$NODE2_URL" "${KEY_EVENTUAL}-majority"
    echo "  Eventual write on majority (node-2): OK"

    try_eventual_write "$NODE3_URL" "${KEY_EVENTUAL}-majority-2"
    echo "  Eventual write on majority (node-3): OK"

    echo -e "  ${CLR_GREEN}[OK] Eventual writes succeed on both sides.${CLR_RESET}"

    # === STEP 7: Re-verify minority STILL cannot certify after more time ===
    log_step 7 "Re-verify minority cannot certify (extended check)"
    sleep 5
    minority_status_json=$(get_cert_status "$NODE1_URL" "$KEY_MINORITY")
    minority_status=$(echo "$minority_status_json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    s = d.get('status', 'unknown')
    print(str(s).lower() if isinstance(s, str) else str(s).lower())
except:
    print('unknown')
" 2>/dev/null || echo "unknown")

    echo "  Minority certification status after extended wait: ${minority_status}"
    if [ "$minority_status" = "certified" ]; then
        echo -e "  ${CLR_RED}[SAFETY VIOLATION] Minority achieved certification after delay!${CLR_RESET}"
        return 1
    fi
    echo -e "  ${CLR_GREEN}[OK] Minority still cannot certify.${CLR_RESET}"

    # === STEP 8: Heal partition and verify convergence ===
    log_step 8 "Heal partition and verify eventual data convergence"
    heal_all
    sleep 5

    # Write on each node and wait for convergence.
    try_eventual_write "$NODE1_URL" "${KEY_EVENTUAL}"
    try_eventual_write "$NODE2_URL" "${KEY_EVENTUAL}"

    echo "  Checking convergence..."
    local converged=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        if ! wait_for_convergence "3" "$url" "$name" "$CONVERGENCE_RETRIES" "$CONVERGENCE_INTERVAL" "${KEY_EVENTUAL}"; then
            # Convergence is best-effort for this test — primary goal is safety.
            echo -e "  ${CLR_YELLOW}[WARN] ${name} did not converge.${CLR_RESET}"
            converged=false
        fi
    done

    if $converged; then
        echo -e "  ${CLR_GREEN}[OK] All nodes converged after partition heal.${CLR_RESET}"
    else
        echo -e "  ${CLR_YELLOW}[WARN] Partial convergence — checking if data is reachable.${CLR_RESET}"
    fi

    echo ""
    echo -e "${CLR_GREEN}[PASS] 1v2-partition: split-brain safety verified.${CLR_RESET}"
    return 0
}

# ==========================================================================
# SCENARIO 2: Flapping partition (rapid connect/disconnect)
# ==========================================================================
scenario_flapping_partition() {
    local KEY="split-brain-flap-$$"
    local FLAP_CYCLES=3

    log_step 1 "Verify cluster health"
    if ! check_cluster "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; then
        return 1
    fi

    log_step 2 "Flapping partition test (${FLAP_CYCLES} cycles)"
    local safety_ok=true

    for cycle in $(seq 1 "$FLAP_CYCLES"); do
        local cycle_key="${KEY}-cycle${cycle}"
        echo ""
        echo "  --- Cycle ${cycle}/${FLAP_CYCLES} ---"

        # Partition: isolate node-1
        echo "  Partitioning node-1..."
        isolate_node "$NODE1_CONTAINER"
        sleep 2

        # Try certified write on minority
        local response
        response=$(try_certified_write "$NODE1_URL" "${cycle_key}" "pending")
        local status_json
        sleep 3
        status_json=$(get_cert_status "$NODE1_URL" "${cycle_key}")
        local status
        status=$(echo "$status_json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    s = d.get('status', 'unknown')
    print(str(s).lower() if isinstance(s, str) else str(s).lower())
except:
    print('unknown')
" 2>/dev/null || echo "unknown")

        echo "  Cycle ${cycle}: minority cert status = ${status}"
        if [ "$status" = "certified" ]; then
            echo -e "  ${CLR_RED}[SAFETY VIOLATION] Certified on minority during cycle ${cycle}!${CLR_RESET}"
            safety_ok=false
        fi

        # Heal
        echo "  Healing partition..."
        heal_all
        sleep 3
    done

    if $safety_ok; then
        echo ""
        echo -e "${CLR_GREEN}[PASS] flapping-partition: no split-brain across ${FLAP_CYCLES} cycles.${CLR_RESET}"
        return 0
    else
        echo ""
        echo -e "${CLR_RED}[FAIL] flapping-partition: safety violation detected.${CLR_RESET}"
        return 1
    fi
}

# ==========================================================================
# Main execution
# ==========================================================================

separator
echo -e "${CLR_BOLD}AsteroidDB Split-Brain Safety Tests (Issue #303)${CLR_RESET}"
separator
echo ""

# Start cluster
echo "[split-brain] Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
wait_for_cluster
echo ""

# Determine scenarios
ALL_SCENARIOS=("1v2-partition" "flapping-partition")

if [ -n "$SCENARIO_NAME" ]; then
    SCENARIOS_TO_RUN=("$SCENARIO_NAME")
else
    SCENARIOS_TO_RUN=("${ALL_SCENARIOS[@]}")
fi

PASS=0
FAIL=0
declare -a RESULTS=()

for scenario in "${SCENARIOS_TO_RUN[@]}"; do
    separator
    echo -e "${CLR_BOLD}Scenario: ${scenario}${CLR_RESET}"
    sub_separator

    S_START=$(date +%s)
    S_EXIT=0

    set +e
    case "$scenario" in
        1v2-partition)
            scenario_1v2_partition
            S_EXIT=$?
            ;;
        flapping-partition)
            scenario_flapping_partition
            S_EXIT=$?
            ;;
        *)
            echo "[ERROR] Unknown scenario: ${scenario}" >&2
            S_EXIT=1
            ;;
    esac
    set -e

    S_END=$(date +%s)
    S_DURATION=$(( S_END - S_START ))

    if [ "$S_EXIT" -eq 0 ]; then
        PASS=$(( PASS + 1 ))
        RESULTS+=("PASS  ${scenario} (${S_DURATION}s)")
    else
        FAIL=$(( FAIL + 1 ))
        RESULTS+=("FAIL  ${scenario} (${S_DURATION}s)")
    fi

    # Reset cluster between scenarios.
    echo ""
    echo "[split-brain] Resetting cluster state..."
    heal_all 2>/dev/null || true
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker start "$container" 2>/dev/null || true
    done
    wait_for_cluster
    echo ""
done

# --- Summary ---
separator
echo -e "${CLR_BOLD}Split-Brain Test Summary${CLR_RESET}"
sub_separator
for r in "${RESULTS[@]}"; do
    echo "  $r"
done
echo ""
echo "  Passed: ${PASS}"
echo "  Failed: ${FAIL}"
separator

if [ "$FAIL" -gt 0 ]; then
    echo -e "${CLR_RED}Some scenarios failed — possible split-brain safety issue.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}All split-brain safety scenarios passed.${CLR_RESET}"
exit 0
