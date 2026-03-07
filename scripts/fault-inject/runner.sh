#!/usr/bin/env bash
# Fault injection test runner for AsteroidDB.
#
# Reads scenario definitions from netem/scenarios.json (fault-inject scenarios
# are those with crash/rejoin/asymmetric/jitter/rolling tags), starts the
# Docker cluster, executes each scenario, and produces a summary report.
#
# Usage: ./scripts/fault-inject/runner.sh [OPTIONS]
#
# Options:
#   --scenario <name>   Run a single scenario by name
#   --all               Run all fault-injection scenarios
#   --list              List available fault-injection scenarios
#   --help              Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - python3 available on the host (used by netem/lib.sh for JSON parsing)
#   - jq installed
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETEM_DIR="${SCRIPT_DIR}/../netem"
COMPOSE_FILE="${SCRIPT_DIR}/../../docker-compose.yml"
SCENARIOS_FILE="${NETEM_DIR}/scenarios.json"

source "${NETEM_DIR}/lib.sh"

# Fault-injection scenario names (those added by this feature).
FAULT_SCENARIOS=("crash-recovery" "asymmetric-partition" "jitter-latency" "rolling-partition" "node-rejoin")

RUN_ALL=false
LIST_ONLY=false
SCENARIO_NAME=""

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/fault-inject/runner.sh [OPTIONS]

Options:
  --scenario <name>   Run a single scenario by name
  --all               Run all fault-injection scenarios
  --list              List available fault-injection scenarios
  --help              Show this help message

Available scenarios:
  crash-recovery        Stop a container, restart, verify convergence
  asymmetric-partition  Block A->B but allow B->A, verify consistency
  jitter-latency        50ms +/- 30ms jitter, verify operations
  rolling-partition     Sequentially isolate each node, heal, verify
  node-rejoin           Stop node, write, restart, verify sync
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
        --list)
            LIST_ONLY=true
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

# --- List mode ---
if [ "$LIST_ONLY" = "true" ]; then
    echo "Available fault-injection scenarios:"
    echo ""
    for name in "${FAULT_SCENARIOS[@]}"; do
        desc=$(jq -r ".scenarios[] | select(.name == \"${name}\") | .description" "$SCENARIOS_FILE")
        printf "  %-25s %s\n" "$name" "$desc"
    done
    echo ""
    exit 0
fi

# --- Validate selection ---
if [ "$RUN_ALL" = "false" ] && [ -z "$SCENARIO_NAME" ]; then
    echo "[ERROR] Specify --all, --scenario <name>, or --list." >&2
    usage >&2
    exit 1
fi

# If a specific scenario is requested, validate it exists.
if [ -n "$SCENARIO_NAME" ]; then
    found=false
    for s in "${FAULT_SCENARIOS[@]}"; do
        if [ "$s" = "$SCENARIO_NAME" ]; then
            found=true
            break
        fi
    done
    if [ "$found" = "false" ]; then
        echo "[ERROR] Unknown scenario: ${SCENARIO_NAME}" >&2
        echo "Valid scenarios: ${FAULT_SCENARIOS[*]}" >&2
        exit 1
    fi
fi

# --- Cluster management ---
cleanup() {
    echo ""
    echo "[fault-inject] Tearing down cluster..."
    # Flush iptables rules in all containers before tearing down
    for container in asteroidb-node-1 asteroidb-node-2 asteroidb-node-3; do
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
    done
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

wait_for_cluster() {
    echo "[fault-inject] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 30); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 30 ]; then
                echo "[fault-inject] ERROR: Node on port ${port} did not become ready"
                exit 1
            fi
            sleep 1
        done
    done
    echo "[fault-inject] Cluster is ready."
}

# --- Start cluster ---
separator
echo -e "${CLR_BOLD}AsteroidDB Fault Injection Tests${CLR_RESET}"
separator
echo ""

echo "[fault-inject] Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
wait_for_cluster
echo ""

# --- Determine scenarios to run ---
if [ "$RUN_ALL" = "true" ]; then
    SCENARIOS_TO_RUN=("${FAULT_SCENARIOS[@]}")
else
    SCENARIOS_TO_RUN=("$SCENARIO_NAME")
fi

# --- Execute scenarios ---
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_START=$(date +%s)
declare -a RESULTS=()

for scenario_name in "${SCENARIOS_TO_RUN[@]}"; do
    script=$(jq -r ".scenarios[] | select(.name == \"${scenario_name}\") | .script" "$SCENARIOS_FILE")
    timeout_secs=$(jq -r ".scenarios[] | select(.name == \"${scenario_name}\") | .timeout_seconds" "$SCENARIOS_FILE")
    script_path="${NETEM_DIR}/${script}"

    separator
    echo -e "${CLR_BOLD}Scenario: ${scenario_name}${CLR_RESET}"
    sub_separator

    if [ ! -f "$script_path" ]; then
        echo "[ERROR] Script not found: ${script_path}" >&2
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        RESULTS+=("FAIL  ${scenario_name} (script not found)")
        continue
    fi

    S_START=$(date +%s)
    S_EXIT=0

    set +e
    if command -v timeout > /dev/null 2>&1; then
        timeout "${timeout_secs}" bash "$script_path"
        S_EXIT=$?
    else
        bash "$script_path"
        S_EXIT=$?
    fi
    set -e

    S_END=$(date +%s)
    S_DURATION=$(( S_END - S_START ))

    if [ "$S_EXIT" -eq 0 ]; then
        echo -e "${CLR_GREEN}[PASS] ${scenario_name} (${S_DURATION}s)${CLR_RESET}"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
        RESULTS+=("PASS  ${scenario_name} (${S_DURATION}s)")
    elif [ "$S_EXIT" -eq 124 ]; then
        echo -e "${CLR_RED}[TIMEOUT] ${scenario_name} (${timeout_secs}s limit)${CLR_RESET}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        RESULTS+=("TIMEOUT  ${scenario_name} (${timeout_secs}s limit)")
    else
        echo -e "${CLR_RED}[FAIL] ${scenario_name} (${S_DURATION}s, exit=${S_EXIT})${CLR_RESET}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        RESULTS+=("FAIL  ${scenario_name} (${S_DURATION}s, exit=${S_EXIT})")
    fi

    echo ""

    # Re-verify cluster is up before next scenario (restart stopped nodes).
    echo "[fault-inject] Ensuring cluster is healthy for next scenario..."
    for container in asteroidb-node-1 asteroidb-node-2 asteroidb-node-3; do
        docker start "$container" 2>/dev/null || true
    done
    # Remove any leftover netem rules.
    for container in asteroidb-node-1 asteroidb-node-2 asteroidb-node-3; do
        "${NETEM_DIR}/remove-netem.sh" "$container" 2>/dev/null || true
    done
    # Flush iptables OUTPUT on all nodes (in case asymmetric partition left rules).
    for container in asteroidb-node-1 asteroidb-node-2 asteroidb-node-3; do
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
    done
    # Wait for health.
    wait_for_cluster
    echo ""
done

# --- Summary ---
TOTAL_END=$(date +%s)
TOTAL_DURATION=$(( TOTAL_END - TOTAL_START ))

separator
echo -e "${CLR_BOLD}Summary${CLR_RESET}"
sub_separator
for r in "${RESULTS[@]}"; do
    echo "  $r"
done
echo ""
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
