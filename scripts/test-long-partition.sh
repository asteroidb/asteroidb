#!/usr/bin/env bash
# Long partition + reconnection convergence test for AsteroidDB.
#
# Spins up a 3-node Docker cluster, injects network partitions of increasing
# duration, writes data on both sides during the partition, heals the network,
# and measures convergence time + verifies CRDT merge correctness.
#
# Tested partition durations: 30s, 60s, 120s, 300s (configurable via CLI).
#
# For each duration the script:
#   1. Partitions node-3 from node-1 and node-2 using iptables
#   2. Writes counter increments and OR-Set additions on BOTH sides
#   3. Heals the partition by flushing iptables rules
#   4. Polls all nodes until convergence or timeout
#   5. Verifies CRDT merge correctness:
#      - Counter: sum of all increments matches on every node
#      - OR-Set: union of all additions present on every node
#   6. Records convergence time and data integrity results
#
# Results are written as JSON to target/long-partition-results.json and printed
# as CSV to stdout.
#
# Usage:
#   ./scripts/test-long-partition.sh [OPTIONS]
#
# Options:
#   --durations "30 60 120 300"   Space-separated partition durations in seconds
#   --writes-per-side N           Counter increments per side per duration (default 20)
#   --set-items-per-side N        OR-Set items per side per duration (default 10)
#   --convergence-timeout N       Max seconds to wait for convergence (default 600)
#   --skip-build                  Skip Docker image build (use existing image)
#   --help                        Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - python3, curl, jq on the host
#   - Ports 3001-3003 free
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
NETEM_DIR="${SCRIPT_DIR}/netem"

source "${NETEM_DIR}/lib.sh"

# --- Defaults ---
DURATIONS=(30 60 120 300)
WRITES_PER_SIDE=20
SET_ITEMS_PER_SIDE=10
CONVERGENCE_TIMEOUT=600
SKIP_BUILD=false

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/test-long-partition.sh [OPTIONS]

Options:
  --durations "30 60 120 300"   Space-separated partition durations in seconds
  --writes-per-side N           Counter increments per side per duration (default 20)
  --set-items-per-side N        OR-Set items per side per duration (default 10)
  --convergence-timeout N       Max seconds to wait for convergence (default 600)
  --skip-build                  Skip Docker image build (use existing image)
  --help                        Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --durations)
            IFS=' ' read -ra DURATIONS <<< "${2:?--durations requires a value}"
            shift 2
            ;;
        --writes-per-side)
            WRITES_PER_SIDE="${2:?--writes-per-side requires a value}"
            shift 2
            ;;
        --set-items-per-side)
            SET_ITEMS_PER_SIDE="${2:?--set-items-per-side requires a value}"
            shift 2
            ;;
        --convergence-timeout)
            CONVERGENCE_TIMEOUT="${2:?--convergence-timeout requires a value}"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
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

# --- Node configuration ---
NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"

# Partition sides:
#   Majority side: node-1 + node-2
#   Minority side: node-3
MAJORITY_URLS=("$NODE1_URL" "$NODE2_URL")
MINORITY_URL="$NODE3_URL"
MINORITY_CONTAINER="$NODE3_CONTAINER"

RUN_ID="lp-$$"
RESULT_FILE="${PROJECT_DIR}/target/long-partition-results.json"

# --- Cleanup trap ---
cleanup() {
    echo ""
    echo "[long-partition] Cleaning up..."
    # Flush iptables on all containers
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
        docker exec "$container" iptables -F INPUT 2>/dev/null || true
    done
    # Remove any leftover netem rules
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        "${NETEM_DIR}/remove-netem.sh" "$container" 2>/dev/null || true
    done
    # Tear down the cluster
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

# --- Helper functions ---

wait_for_cluster() {
    echo "[long-partition] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 60); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 60 ]; then
                echo "[long-partition] ERROR: Node on port ${port} did not become ready" >&2
                exit 1
            fi
            sleep 2
        done
    done
    echo "[long-partition] Cluster is ready."
}

# write_counter <url> <key> <count>
# Increment a counter key N times.
write_counter() {
    local url="$1"
    local key="$2"
    local count="$3"
    for _ in $(seq 1 "$count"); do
        curl -sf -X POST "${url}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" > /dev/null || true
    done
}

# write_set_add <url> <key> <item>
# Add an item to an OR-Set.
write_set_add() {
    local url="$1"
    local key="$2"
    local item="$3"
    curl -sf -X POST "${url}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"set_add\",\"key\":\"${key}\",\"value\":\"${item}\"}" > /dev/null || true
}

# read_set <url> <key>
# Read an OR-Set value. Returns the raw JSON.
read_set() {
    local url="$1"
    local key="$2"
    curl -sf --max-time 10 "${url}/api/eventual/${key}" 2>/dev/null || echo '{"value":null}'
}

# extract_set_items <json>
# Extract set items from API JSON response as newline-separated list.
extract_set_items() {
    local json="$1"
    echo "$json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        pass
    elif isinstance(v, dict):
        items = v.get('value', [])
        if isinstance(items, list):
            for item in sorted(items):
                print(item)
    elif isinstance(v, list):
        for item in sorted(v):
            print(item)
except Exception:
    pass
" 2>/dev/null || true
}

# count_set_items <json>
# Count items in an OR-Set response.
count_set_items() {
    local json="$1"
    echo "$json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        print(0)
    elif isinstance(v, dict):
        items = v.get('value', [])
        if isinstance(items, list):
            print(len(items))
        else:
            print(0)
    elif isinstance(v, list):
        print(len(v))
    else:
        print(0)
except Exception:
    print(0)
" 2>/dev/null || echo "0"
}

# partition_node3 - Block bidirectional traffic between node-3 and (node-1, node-2).
partition_node3() {
    local node1_ip node2_ip
    node1_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NODE1_CONTAINER")
    node2_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NODE2_CONTAINER")

    echo "  Partition: node-1 IP=${node1_ip}, node-2 IP=${node2_ip}"

    # Block node-3 -> (node-1, node-2) and reverse
    docker exec "$MINORITY_CONTAINER" iptables -A OUTPUT -d "$node1_ip" -j DROP
    docker exec "$MINORITY_CONTAINER" iptables -A OUTPUT -d "$node2_ip" -j DROP
    docker exec "$MINORITY_CONTAINER" iptables -A INPUT -s "$node1_ip" -j DROP
    docker exec "$MINORITY_CONTAINER" iptables -A INPUT -s "$node2_ip" -j DROP
}

# heal_node3 - Remove iptables rules on node-3 to restore connectivity.
heal_node3() {
    docker exec "$MINORITY_CONTAINER" iptables -F OUTPUT
    docker exec "$MINORITY_CONTAINER" iptables -F INPUT
}

# check_counter_convergence <key> <expected> <url> <name>
# Returns 0 if the counter value matches expected, 1 otherwise.
check_counter_value() {
    local key="$1"
    local expected="$2"
    local url="$3"
    local json val
    json=$(read_counter "$url" "$key")
    val=$(extract_value "$json")
    [ "$val" = "$expected" ]
}

# check_set_convergence <key> <expected_count> <url>
# Returns 0 if the set has the expected number of items.
check_set_count() {
    local key="$1"
    local expected_count="$2"
    local url="$3"
    local json count
    json=$(read_set "$url" "$key")
    count=$(count_set_items "$json")
    [ "$count" -eq "$expected_count" ]
}

# poll_convergence <counter_key> <expected_counter> <set_key> <expected_set_count> <timeout>
# Poll all 3 nodes until both counter and set converge, or timeout.
# Returns: convergence time in milliseconds, or -1 if timeout.
poll_convergence() {
    local counter_key="$1"
    local expected_counter="$2"
    local set_key="$3"
    local expected_set_count="$4"
    local timeout_secs="$5"

    local heal_ms
    heal_ms=$(now_epoch_ms)

    for attempt in $(seq 1 "$timeout_secs"); do
        sleep 1
        local all_converged=true

        # Check counter on all nodes
        for url in "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; do
            if ! check_counter_value "$counter_key" "$expected_counter" "$url"; then
                all_converged=false
                break
            fi
        done

        # Check set on all nodes (only if counter already converged)
        if $all_converged; then
            for url in "$NODE1_URL" "$NODE2_URL" "$NODE3_URL"; do
                if ! check_set_count "$set_key" "$expected_set_count" "$url"; then
                    all_converged=false
                    break
                fi
            done
        fi

        local elapsed
        elapsed=$(elapsed_ms "$heal_ms")

        if $all_converged; then
            echo "$elapsed"
            return 0
        fi

        # Progress update every 10 seconds
        if [ $(( attempt % 10 )) -eq 0 ]; then
            # Get current values for reporting
            local n3_counter_json n3_counter_val n3_set_json n3_set_count
            n3_counter_json=$(read_counter "$NODE3_URL" "$counter_key")
            n3_counter_val=$(extract_value "$n3_counter_json")
            n3_set_json=$(read_set "$NODE3_URL" "$set_key")
            n3_set_count=$(count_set_items "$n3_set_json")
            echo "  [${elapsed}ms] node-3: counter=${n3_counter_val}/${expected_counter}, set=${n3_set_count}/${expected_set_count}" >&2
        fi
    done

    # Timeout
    echo "-1"
    return 1
}

# verify_crdt_correctness <counter_key> <expected_counter> <set_key> <expected_items_file>
# Verify all nodes have correct CRDT state after convergence.
# Returns 0 if correct, 1 if mismatch found.
verify_crdt_correctness() {
    local counter_key="$1"
    local expected_counter="$2"
    local set_key="$3"
    local expected_items_file="$4"
    local all_correct=true

    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"

        # Check counter
        local counter_json counter_val
        counter_json=$(read_counter "$url" "$counter_key")
        counter_val=$(extract_value "$counter_json")
        if [ "$counter_val" = "$expected_counter" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name} counter = ${counter_val} (expected ${expected_counter})${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name} counter = ${counter_val} (expected ${expected_counter})${CLR_RESET}"
            all_correct=false
        fi

        # Check set
        local set_json set_items
        set_json=$(read_set "$url" "$set_key")
        set_items=$(extract_set_items "$set_json")
        local actual_count expected_count
        actual_count=$(echo "$set_items" | grep -c '.' || echo "0")
        expected_count=$(grep -c '.' "$expected_items_file" || echo "0")

        # Compare sorted item lists
        local diff_output
        diff_output=$(diff <(echo "$set_items" | sort) <(sort "$expected_items_file") 2>/dev/null || true)
        if [ -z "$diff_output" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name} set has ${actual_count}/${expected_count} items (union correct)${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name} set mismatch: ${actual_count}/${expected_count} items${CLR_RESET}"
            echo "  Missing/extra items:"
            echo "$diff_output" | head -10
            all_correct=false
        fi
    done

    $all_correct
}

# run_partition_scenario <duration_secs>
# Run a single partition scenario with the given duration.
# Returns 0 on success, 1 on failure.
# Outputs JSON result object to stdout.
run_partition_scenario() {
    local duration="$1"
    local scenario_id="${RUN_ID}-${duration}s"
    local counter_key="${scenario_id}-counter"
    local set_key="${scenario_id}-set"
    local expected_items_file
    expected_items_file=$(mktemp)

    local total_counter_expected=$(( WRITES_PER_SIDE * 2 ))
    local total_set_expected=$(( SET_ITEMS_PER_SIDE * 2 ))

    separator
    echo -e "${CLR_BOLD}Partition Duration: ${duration}s${CLR_RESET}"
    sub_separator
    echo "  Counter writes per side: ${WRITES_PER_SIDE}"
    echo "  Set items per side:      ${SET_ITEMS_PER_SIDE}"
    echo "  Expected counter total:  ${total_counter_expected}"
    echo "  Expected set items:      ${total_set_expected}"
    echo ""

    # --- Phase 1: Partition ---
    log_step 1 "Create iptables partition (node-3 isolated)"
    partition_node3
    local partition_start_ms
    partition_start_ms=$(now_epoch_ms)
    echo "  Partition active at $(date '+%H:%M:%S')."
    echo ""

    # --- Phase 2: Write on both sides ---
    log_step 2 "Write data on both sides during partition"

    # Majority side: write to node-1
    echo "  Writing ${WRITES_PER_SIDE} counter increments to node-1 (majority side)..."
    write_counter "$NODE1_URL" "$counter_key" "$WRITES_PER_SIDE"

    echo "  Writing ${SET_ITEMS_PER_SIDE} set items to node-1 (majority side)..."
    for i in $(seq 1 "$SET_ITEMS_PER_SIDE"); do
        local item="majority-item-${i}"
        write_set_add "$NODE1_URL" "$set_key" "$item"
        echo "$item" >> "$expected_items_file"
    done

    # Minority side: write to node-3
    echo "  Writing ${WRITES_PER_SIDE} counter increments to node-3 (minority side)..."
    write_counter "$MINORITY_URL" "$counter_key" "$WRITES_PER_SIDE"

    echo "  Writing ${SET_ITEMS_PER_SIDE} set items to node-3 (minority side)..."
    for i in $(seq 1 "$SET_ITEMS_PER_SIDE"); do
        local item="minority-item-${i}"
        write_set_add "$MINORITY_URL" "$set_key" "$item"
        echo "$item" >> "$expected_items_file"
    done

    echo "  All writes complete."
    echo ""

    # --- Phase 3: Hold partition for remaining duration ---
    log_step 3 "Hold partition for ${duration}s total"
    local elapsed_ms remaining_ms
    elapsed_ms=$(( $(now_epoch_ms) - partition_start_ms ))
    remaining_ms=$(( duration * 1000 - elapsed_ms ))

    if [ "$remaining_ms" -gt 0 ]; then
        local remaining_secs
        remaining_secs=$(python3 -c "print(round(${remaining_ms}/1000, 1))")
        echo "  Writes took $(python3 -c "print(round(${elapsed_ms}/1000, 1))")s. Holding for ${remaining_secs}s more..."
        sleep "$(python3 -c "print(${remaining_ms}/1000)")"
    else
        echo "  Writes took longer than partition duration; healing immediately."
    fi

    local partition_end_ms
    partition_end_ms=$(now_epoch_ms)
    local actual_partition_ms=$(( partition_end_ms - partition_start_ms ))
    echo "  Actual partition duration: $(python3 -c "print(round(${actual_partition_ms}/1000, 1))")s"
    echo ""

    # --- Phase 4: Heal ---
    log_step 4 "Heal partition (flush iptables on node-3)"
    heal_node3
    local heal_ms
    heal_ms=$(now_epoch_ms)
    echo "  Partition healed at $(date '+%H:%M:%S'). Starting convergence timer..."
    echo ""

    # --- Phase 5: Measure convergence ---
    log_step 5 "Measure convergence (timeout: ${CONVERGENCE_TIMEOUT}s)"
    local convergence_ms convergence_ok
    convergence_ok=true

    set +e
    convergence_ms=$(poll_convergence "$counter_key" "$total_counter_expected" \
        "$set_key" "$total_set_expected" "$CONVERGENCE_TIMEOUT")
    local poll_exit=$?
    set -e

    if [ "$poll_exit" -ne 0 ]; then
        convergence_ms=$(elapsed_ms "$heal_ms")
        convergence_ok=false
        echo -e "  ${CLR_RED}[TIMEOUT] Did not converge within ${CONVERGENCE_TIMEOUT}s.${CLR_RESET}"
    else
        echo -e "  ${CLR_GREEN}[OK] All nodes converged in ${convergence_ms}ms.${CLR_RESET}"
    fi
    echo ""

    # --- Phase 6: Verify correctness ---
    log_step 6 "Verify CRDT merge correctness"
    local correctness_ok=true
    if ! verify_crdt_correctness "$counter_key" "$total_counter_expected" \
        "$set_key" "$expected_items_file"; then
        correctness_ok=false
    fi
    echo ""

    # Clean up temp file
    rm -f "$expected_items_file"

    # --- Build result JSON ---
    local converged_bool="true"
    if [ "$convergence_ok" = "false" ]; then converged_bool="false"; fi
    local correct_bool="true"
    if [ "$correctness_ok" = "false" ]; then correct_bool="false"; fi

    # Write result JSON to the shared temp file (SCENARIO_RESULT_FILE).
    cat > "$SCENARIO_RESULT_FILE" <<EOF
{
    "partition_duration_secs": ${duration},
    "actual_partition_ms": ${actual_partition_ms},
    "convergence_time_ms": ${convergence_ms},
    "converged": ${converged_bool},
    "data_correct": ${correct_bool},
    "counter_writes_per_side": ${WRITES_PER_SIDE},
    "set_items_per_side": ${SET_ITEMS_PER_SIDE},
    "expected_counter_total": ${total_counter_expected},
    "expected_set_items": ${total_set_expected}
}
EOF

    if [ "$convergence_ok" = "true" ] && [ "$correctness_ok" = "true" ]; then
        return 0
    else
        return 1
    fi
}

# ======================================================================
# Main
# ======================================================================
separator
echo -e "${CLR_BOLD}AsteroidDB Long Partition + Reconnection Convergence Test${CLR_RESET}"
separator
echo ""
echo "  Partition durations:     ${DURATIONS[*]}s"
echo "  Counter writes/side:     ${WRITES_PER_SIDE}"
echo "  Set items/side:          ${SET_ITEMS_PER_SIDE}"
echo "  Convergence timeout:     ${CONVERGENCE_TIMEOUT}s"
echo "  Run ID:                  ${RUN_ID}"
echo ""

# --- Start cluster ---
echo "[long-partition] Starting cluster..."
if [ "$SKIP_BUILD" = "true" ]; then
    docker compose -f "$COMPOSE_FILE" up -d --quiet-pull 2>&1 | tail -5
else
    docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
fi
wait_for_cluster
echo ""

# --- Run scenarios ---
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_START=$(date +%s)
declare -a RESULT_JSONS=()
declare -a CSV_ROWS=()

# Temp file used by run_partition_scenario to pass result JSON back.
SCENARIO_RESULT_FILE=$(mktemp)
trap 'rm -f "$SCENARIO_RESULT_FILE"; cleanup' EXIT

CSV_ROWS+=("partition_secs,actual_partition_ms,convergence_ms,converged,data_correct")

for duration in "${DURATIONS[@]}"; do
    SCENARIO_EXIT=0
    echo "" > "$SCENARIO_RESULT_FILE"

    set +e
    run_partition_scenario "$duration"
    SCENARIO_EXIT=$?
    set -e

    # Read the result JSON from the temp file
    RESULT_JSON=$(cat "$SCENARIO_RESULT_FILE" 2>/dev/null || echo "{}")

    # Parse result for CSV
    if [ -n "$RESULT_JSON" ] && [ "$RESULT_JSON" != "{}" ]; then
        RESULT_JSONS+=("$RESULT_JSON")
        local_csv=$(echo "$RESULT_JSON" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print('{},{},{},{},{}'.format(
        d.get('partition_duration_secs', '?'),
        d.get('actual_partition_ms', '?'),
        d.get('convergence_time_ms', '?'),
        d.get('converged', '?'),
        d.get('data_correct', '?')
    ))
except Exception:
    print('?,?,?,?,?')
" 2>/dev/null || echo "?,?,?,?,?")
        CSV_ROWS+=("$local_csv")
    fi

    if [ "$SCENARIO_EXIT" -eq 0 ]; then
        echo -e "${CLR_GREEN}[PASS] Partition ${duration}s scenario${CLR_RESET}"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        echo -e "${CLR_RED}[FAIL] Partition ${duration}s scenario${CLR_RESET}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi
    echo ""

    # Restore cluster health between scenarios
    echo "[long-partition] Ensuring cluster is healthy for next scenario..."
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
        docker exec "$container" iptables -F INPUT 2>/dev/null || true
    done
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        "${NETEM_DIR}/remove-netem.sh" "$container" 2>/dev/null || true
    done
    wait_for_cluster
    echo ""
done

# ======================================================================
# Output results
# ======================================================================
TOTAL_END=$(date +%s)
TOTAL_DURATION=$(( TOTAL_END - TOTAL_START ))

separator
echo -e "${CLR_BOLD}Results (CSV)${CLR_RESET}"
sub_separator
for row in "${CSV_ROWS[@]}"; do
    echo "$row"
done
echo ""

# Build combined JSON results
separator
echo -e "${CLR_BOLD}Results (JSON)${CLR_RESET}"
sub_separator

# Write individual result JSONs to a temp file, one per line
RESULTS_TMP=$(mktemp)
for rj in "${RESULT_JSONS[@]}"; do
    echo "$rj" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)))" >> "$RESULTS_TMP" 2>/dev/null || true
done

COMBINED_JSON=$(python3 -c "
import json, sys

results = []
with open('${RESULTS_TMP}') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                pass

output = {
    'test': 'long-partition-recovery',
    'run_id': '${RUN_ID}',
    'total_duration_secs': ${TOTAL_DURATION},
    'passed': ${PASS_COUNT},
    'failed': ${FAIL_COUNT},
    'scenarios': results
}

print(json.dumps(output, indent=2))
" 2>/dev/null || echo '{"error": "failed to build combined JSON"}')
rm -f "$RESULTS_TMP"

echo "$COMBINED_JSON"
echo ""

# Write results to file
mkdir -p "$(dirname "$RESULT_FILE")"
echo "$COMBINED_JSON" > "$RESULT_FILE"
echo "  Results written to: ${RESULT_FILE}"

# ======================================================================
# Summary
# ======================================================================
separator
echo -e "${CLR_BOLD}Summary${CLR_RESET}"
sub_separator
echo "  Partitions tested: ${#DURATIONS[@]}"
echo "  Passed:            ${PASS_COUNT}"
echo "  Failed:            ${FAIL_COUNT}"
echo "  Total time:        ${TOTAL_DURATION}s"
separator

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${CLR_RED}Some scenarios failed.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}All long partition scenarios passed.${CLR_RESET}"
exit 0
