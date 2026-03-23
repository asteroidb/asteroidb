#!/usr/bin/env bash
# Satellite constellation scenario tests for AsteroidDB.
#
# Simulates space-like network conditions across a 3-node Docker cluster:
#
#   Scenario 1 (LEO):  20-40ms RTT, 1-5% packet loss, intermittent blackouts
#   Scenario 2 (MEO):  100-150ms RTT, 0.5% packet loss, stable connectivity
#   Scenario 3 (GEO):  500-600ms RTT, 0.1% packet loss, stable connectivity
#   Scenario 4 (ISL):  50-80ms RTT, +/-20ms jitter, variable conditions
#   Scenario 5 (Handover): periodic link breaks every 90s simulating LEO pass
#
# Each scenario measures:
#   - Delta sync convergence time (ms)
#   - Write throughput (ops/s)
#   - Read throughput (ops/s)
#   - Data integrity (CRDT convergence correctness)
#
# Usage: ./scripts/test-satellite-scenario.sh [OPTIONS]
#
# Options:
#   --scenario <name>   Run a specific scenario (leo|meo|geo|isl|handover)
#   --quick             Use reduced write counts for faster execution
#   --help              Show this help message
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
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"

COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"
RESULTS_DIR="${SCRIPT_DIR}/../target/satellite-results"

PASS_COUNT=0
FAIL_COUNT=0
TOTAL_START=$(date +%s)

# Default write counts (overridden with --quick)
WRITE_COUNT=20
QUICK_WRITE_COUNT=5
CONVERGENCE_RETRIES=30
CONVERGENCE_INTERVAL=2

# --- Argument parsing ---
RUN_SCENARIO=""
QUICK_MODE=false

usage() {
    cat <<'USAGE'
Usage: ./scripts/test-satellite-scenario.sh [OPTIONS]

Options:
  --scenario <name>   Run a specific scenario (leo|meo|geo|isl|handover)
  --quick             Use reduced write counts for faster execution
  --help              Show this help message

Scenarios:
  leo       LEO orbit: 20-40ms RTT, 1-5% loss, intermittent blackouts
  meo       MEO orbit: 100-150ms RTT, 0.5% loss, stable
  geo       GEO orbit: 500-600ms RTT, 0.1% loss, stable
  isl       Inter-satellite link: 50-80ms RTT, +/-20ms jitter
  handover  Ground-to-LEO handover: periodic link breaks every 90s
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario)
            RUN_SCENARIO="${2:?--scenario requires a name argument}"
            shift 2
            ;;
        --quick)
            QUICK_MODE=true
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

if [ "$QUICK_MODE" = "true" ]; then
    WRITE_COUNT=$QUICK_WRITE_COUNT
    CONVERGENCE_RETRIES=15
fi

# --- Helper functions ---

cleanup() {
    echo ""
    echo "[satellite] Tearing down cluster..."
    # Remove any lingering netem/iptables rules
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker exec "$container" tc qdisc del dev eth0 root 2>/dev/null || true
        docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
        docker exec "$container" iptables -F INPUT 2>/dev/null || true
    done
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

wait_for_cluster() {
    echo "[satellite] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 30); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 30 ]; then
                echo "[satellite] ERROR: Node on port ${port} did not become ready"
                exit 1
            fi
            sleep 1
        done
    done
    echo "[satellite] Cluster is ready."
}

# ensure_tc <container> - install iproute2 if tc is missing
ensure_tc() {
    local container="$1"
    if ! docker exec "$container" which tc > /dev/null 2>&1; then
        echo "[netem] tc not found in ${container}, installing iproute2..."
        docker exec "$container" bash -c "apt-get update -qq && apt-get install -y -qq iproute2 iptables > /dev/null 2>&1"
    fi
}

# apply_netem <container> <netem_args...>
# Applies tc netem rules to the given container.
apply_netem() {
    local container="$1"
    shift
    ensure_tc "$container"
    docker exec "$container" tc qdisc del dev eth0 root 2>/dev/null || true
    docker exec "$container" tc qdisc add dev eth0 root netem "$@"
    echo "[netem] ${container}: applied netem $*"
}

# remove_all_netem - clear netem rules on all containers
remove_all_netem() {
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        docker exec "$container" tc qdisc del dev eth0 root 2>/dev/null || true
    done
    echo "[netem] All netem rules removed."
}

# apply_blackout <container> - drop 100% packets via iptables
apply_blackout() {
    local container="$1"
    ensure_tc "$container"
    docker exec "$container" iptables -A OUTPUT -j DROP 2>/dev/null || true
    docker exec "$container" iptables -A INPUT -j DROP 2>/dev/null || true
    echo "[blackout] ${container}: link blacked out."
}

# remove_blackout <container> - restore iptables
remove_blackout() {
    local container="$1"
    docker exec "$container" iptables -F OUTPUT 2>/dev/null || true
    docker exec "$container" iptables -F INPUT 2>/dev/null || true
    echo "[blackout] ${container}: link restored."
}

# write_counter_key <url> <key> [count]
write_counter_key() {
    local url="$1"
    local key="$2"
    local count="${3:-1}"
    for _ in $(seq 1 "$count"); do
        curl -sf -X POST "${url}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" > /dev/null
    done
}

# measure_write_throughput <url> <key_prefix> <count>
# Writes <count> counter increments and prints ops/s.
measure_write_throughput() {
    local url="$1"
    local key_prefix="$2"
    local count="$3"

    local start_ms
    start_ms=$(now_epoch_ms)

    for i in $(seq 1 "$count"); do
        curl -sf -X POST "${url}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${key_prefix}\"}" > /dev/null
    done

    local elapsed_ms
    elapsed_ms=$(elapsed_ms "$start_ms")
    if [ "$elapsed_ms" -gt 0 ]; then
        local ops_per_sec
        ops_per_sec=$(python3 -c "print(f'{${count} / (${elapsed_ms} / 1000):.1f}')")
        echo "$ops_per_sec"
    else
        echo "$count"
    fi
}

# measure_read_throughput <url> <key> <count>
# Reads a key <count> times and prints ops/s.
measure_read_throughput() {
    local url="$1"
    local key="$2"
    local count="$3"

    local start_ms
    start_ms=$(now_epoch_ms)

    for _ in $(seq 1 "$count"); do
        curl -sf --max-time 5 "${url}/api/eventual/${key}" > /dev/null 2>&1 || true
    done

    local elapsed_ms
    elapsed_ms=$(elapsed_ms "$start_ms")
    if [ "$elapsed_ms" -gt 0 ]; then
        local ops_per_sec
        ops_per_sec=$(python3 -c "print(f'{${count} / (${elapsed_ms} / 1000):.1f}')")
        echo "$ops_per_sec"
    else
        echo "$count"
    fi
}

# measure_convergence <expected> <key> <url> <name>
# Measures convergence time in ms. Returns 0 on success, 1 on timeout.
# Sets global CONVERGENCE_TIME_MS.
measure_convergence() {
    local expected="$1"
    local key="$2"
    local url="$3"
    local name="$4"

    local start_ms
    start_ms=$(now_epoch_ms)

    for attempt in $(seq 1 "$CONVERGENCE_RETRIES"); do
        local json val
        json=$(read_counter "$url" "$key")
        val=$(extract_value "$json")
        if [ "$val" = "$expected" ]; then
            CONVERGENCE_TIME_MS=$(elapsed_ms "$start_ms")
            echo -e "  ${CLR_GREEN}[OK] ${name} converged to ${expected} in ${CONVERGENCE_TIME_MS}ms${CLR_RESET}"
            return 0
        fi
        sleep "$CONVERGENCE_INTERVAL"
    done

    CONVERGENCE_TIME_MS=$(elapsed_ms "$start_ms")
    local json val
    json=$(read_counter "$url" "$key")
    val=$(extract_value "$json")
    echo -e "  ${CLR_RED}[FAIL] ${name} did not converge (got ${val}, expected ${expected}) after ${CONVERGENCE_TIME_MS}ms${CLR_RESET}"
    return 1
}

# scenario_result <name> <exit_code> <start_time>
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

# write_json_result <scenario> <status> <convergence_ms> <write_ops> <read_ops> <data_ok>
write_json_result() {
    local scenario="$1"
    local status="$2"
    local convergence_ms="$3"
    local write_ops="$4"
    local read_ops="$5"
    local data_ok="$6"
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    local result_file="${RESULTS_DIR}/${scenario}.json"

    cat > "$result_file" <<EOF
{
  "timestamp": "${timestamp}",
  "scenario": "${scenario}",
  "status": "${status}",
  "convergence_time_ms": ${convergence_ms},
  "write_throughput_ops_per_sec": ${write_ops},
  "read_throughput_ops_per_sec": ${read_ops},
  "data_integrity_ok": ${data_ok}
}
EOF
    echo "  Results written to: ${result_file}"
}

# ======================================================================
# Scenario 1: LEO (Low Earth Orbit)
#   - 20-40ms RTT, 1-5% packet loss, intermittent connectivity
#   - Simulates: 60s visible window, then 30s blackout
# ======================================================================
run_scenario_leo() {
    local key="sat-leo-$$"
    local exit_code=0
    local write_ops=0
    local read_ops=0
    CONVERGENCE_TIME_MS=0

    # Trap: clean up netem/iptables on return
    trap 'remove_all_netem; remove_blackout "$NODE2_CONTAINER" 2>/dev/null || true' RETURN

    # --- Apply LEO conditions to node-2 (satellite) ---
    # 30ms delay (20-40ms RTT range), 3% loss, 10ms jitter
    echo "[LEO] Applying LEO conditions to node-2 (satellite node)..."
    apply_netem "$NODE2_CONTAINER" delay 30ms 10ms distribution normal loss 3%

    # --- Phase 1: Write telemetry during visible window ---
    echo "[LEO] Phase 1: Telemetry writes during visible window (60s simulated)..."
    echo "[LEO] Writing ${WRITE_COUNT} increments to satellite node (node-2)..."
    write_ops=$(measure_write_throughput "$NODE2_URL" "$key" "$WRITE_COUNT")
    echo "  Write throughput: ${write_ops} ops/s"

    # Wait for initial sync
    echo "[LEO] Waiting for delta sync..."
    sleep 5

    # Measure convergence to ground station (node-1)
    echo "[LEO] Measuring convergence to ground station (node-1)..."
    if ! measure_convergence "$WRITE_COUNT" "$key" "$NODE1_URL" "ground-station"; then
        exit_code=1
    fi
    local conv1=$CONVERGENCE_TIME_MS

    # --- Phase 2: Blackout (30s simulated, using 10s for test) ---
    echo ""
    echo "[LEO] Phase 2: Satellite blackout (orbit behind Earth)..."
    apply_blackout "$NODE2_CONTAINER"

    # Write additional data to ground station during blackout
    local blackout_key="sat-leo-blackout-$$"
    echo "[LEO] Writing ${WRITE_COUNT} increments to ground station during blackout..."
    write_counter_key "$NODE1_URL" "$blackout_key" "$WRITE_COUNT"

    # Hold blackout for 10s (simulating 30s real blackout, compressed for test)
    echo "[LEO] Holding blackout for 10s..."
    sleep 10

    # --- Phase 3: Restore contact ---
    echo ""
    echo "[LEO] Phase 3: Restoring satellite contact..."
    remove_blackout "$NODE2_CONTAINER"

    # Measure convergence of blackout writes to satellite
    echo "[LEO] Measuring convergence of blackout writes to satellite (node-2)..."
    if ! measure_convergence "$WRITE_COUNT" "$blackout_key" "$NODE2_URL" "satellite"; then
        exit_code=1
    fi
    local conv2=$CONVERGENCE_TIME_MS

    # Read throughput measurement
    echo "[LEO] Measuring read throughput..."
    read_ops=$(measure_read_throughput "$NODE1_URL" "$key" "$WRITE_COUNT")
    echo "  Read throughput: ${read_ops} ops/s"

    # --- Data integrity check ---
    echo "[LEO] Verifying data integrity across all nodes..."
    local data_ok=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local json val
        json=$(read_counter "$url" "$key")
        val=$(extract_value "$json")
        if [ "$val" = "$WRITE_COUNT" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name}: ${key} = ${val}${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name}: ${key} = ${val} (expected ${WRITE_COUNT})${CLR_RESET}"
            data_ok=false
            exit_code=1
        fi
    done

    # Average convergence
    local avg_conv=$(( (conv1 + conv2) / 2 ))
    echo ""
    echo "[LEO] Summary:"
    echo "  Avg convergence: ${avg_conv}ms"
    echo "  Write throughput: ${write_ops} ops/s"
    echo "  Read throughput: ${read_ops} ops/s"
    echo "  Data integrity: ${data_ok}"

    local status="pass"
    [ "$exit_code" -ne 0 ] && status="fail"
    write_json_result "leo" "$status" "$avg_conv" "$write_ops" "$read_ops" "$data_ok"

    return "$exit_code"
}

# ======================================================================
# Scenario 2: MEO (Medium Earth Orbit)
#   - 100-150ms RTT, 0.5% packet loss, stable connectivity
# ======================================================================
run_scenario_meo() {
    local key="sat-meo-$$"
    local exit_code=0
    local write_ops=0
    local read_ops=0
    CONVERGENCE_TIME_MS=0

    trap 'remove_all_netem' RETURN

    # --- Apply MEO conditions to node-2 ---
    # 125ms delay (100-150ms RTT range), 0.5% loss, 25ms jitter
    echo "[MEO] Applying MEO conditions to node-2..."
    apply_netem "$NODE2_CONTAINER" delay 125ms 25ms distribution normal loss 0.5%

    # --- Write data to satellite ---
    echo "[MEO] Writing ${WRITE_COUNT} increments to satellite node (node-2)..."
    write_ops=$(measure_write_throughput "$NODE2_URL" "$key" "$WRITE_COUNT")
    echo "  Write throughput: ${write_ops} ops/s"

    # Wait for sync to propagate through the higher-latency link
    echo "[MEO] Waiting for delta sync to propagate..."
    sleep 8

    # --- Measure convergence ---
    echo "[MEO] Measuring convergence to ground station (node-1)..."
    if ! measure_convergence "$WRITE_COUNT" "$key" "$NODE1_URL" "ground-station"; then
        exit_code=1
    fi
    local conv=$CONVERGENCE_TIME_MS

    # Also verify node-3
    echo "[MEO] Measuring convergence to node-3..."
    if ! measure_convergence "$WRITE_COUNT" "$key" "$NODE3_URL" "node-3"; then
        exit_code=1
    fi

    # Read throughput
    echo "[MEO] Measuring read throughput..."
    read_ops=$(measure_read_throughput "$NODE1_URL" "$key" "$WRITE_COUNT")
    echo "  Read throughput: ${read_ops} ops/s"

    # --- Data integrity ---
    echo "[MEO] Verifying data integrity..."
    local data_ok=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local json val
        json=$(read_counter "$url" "$key")
        val=$(extract_value "$json")
        if [ "$val" = "$WRITE_COUNT" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name}: ${key} = ${val}${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name}: ${key} = ${val} (expected ${WRITE_COUNT})${CLR_RESET}"
            data_ok=false
            exit_code=1
        fi
    done

    echo ""
    echo "[MEO] Summary:"
    echo "  Convergence: ${conv}ms"
    echo "  Write throughput: ${write_ops} ops/s"
    echo "  Read throughput: ${read_ops} ops/s"
    echo "  Data integrity: ${data_ok}"

    local status="pass"
    [ "$exit_code" -ne 0 ] && status="fail"
    write_json_result "meo" "$status" "$conv" "$write_ops" "$read_ops" "$data_ok"

    return "$exit_code"
}

# ======================================================================
# Scenario 3: GEO (Geostationary Orbit)
#   - 500-600ms RTT, 0.1% packet loss, stable connectivity
# ======================================================================
run_scenario_geo() {
    local key="sat-geo-$$"
    local exit_code=0
    local write_ops=0
    local read_ops=0
    CONVERGENCE_TIME_MS=0

    trap 'remove_all_netem' RETURN

    # --- Apply GEO conditions to node-2 ---
    # 550ms delay (500-600ms RTT range), 0.1% loss, 50ms jitter
    echo "[GEO] Applying GEO conditions to node-2..."
    apply_netem "$NODE2_CONTAINER" delay 550ms 50ms distribution normal loss 0.1%

    # --- Write data ---
    echo "[GEO] Writing ${WRITE_COUNT} increments to satellite node (node-2)..."
    write_ops=$(measure_write_throughput "$NODE2_URL" "$key" "$WRITE_COUNT")
    echo "  Write throughput: ${write_ops} ops/s"

    # GEO has very high latency; give more time for sync
    echo "[GEO] Waiting for delta sync (high-latency link)..."
    sleep 15

    # --- Measure convergence ---
    echo "[GEO] Measuring convergence to ground station (node-1)..."
    if ! measure_convergence "$WRITE_COUNT" "$key" "$NODE1_URL" "ground-station"; then
        exit_code=1
    fi
    local conv=$CONVERGENCE_TIME_MS

    # Read throughput
    echo "[GEO] Measuring read throughput..."
    read_ops=$(measure_read_throughput "$NODE1_URL" "$key" "$WRITE_COUNT")
    echo "  Read throughput: ${read_ops} ops/s"

    # --- Data integrity ---
    echo "[GEO] Verifying data integrity..."
    local data_ok=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local json val
        json=$(read_counter "$url" "$key")
        val=$(extract_value "$json")
        if [ "$val" = "$WRITE_COUNT" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name}: ${key} = ${val}${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name}: ${key} = ${val} (expected ${WRITE_COUNT})${CLR_RESET}"
            data_ok=false
            exit_code=1
        fi
    done

    echo ""
    echo "[GEO] Summary:"
    echo "  Convergence: ${conv}ms"
    echo "  Write throughput: ${write_ops} ops/s"
    echo "  Read throughput: ${read_ops} ops/s"
    echo "  Data integrity: ${data_ok}"

    local status="pass"
    [ "$exit_code" -ne 0 ] && status="fail"
    write_json_result "geo" "$status" "$conv" "$write_ops" "$read_ops" "$data_ok"

    return "$exit_code"
}

# ======================================================================
# Scenario 4: Inter-Satellite Link (ISL)
#   - 50-80ms RTT, +/-20ms jitter, variable conditions
#   - Simulates direct laser link between two LEO satellites
# ======================================================================
run_scenario_isl() {
    local key="sat-isl-$$"
    local exit_code=0
    local write_ops=0
    local read_ops=0
    CONVERGENCE_TIME_MS=0

    trap 'remove_all_netem' RETURN

    # --- Apply ISL conditions ---
    # node-2 to node-1 link: 65ms delay (50-80ms range), 20ms jitter
    # node-3 acts as relay with additional delay
    echo "[ISL] Applying inter-satellite link conditions..."
    apply_netem "$NODE2_CONTAINER" delay 65ms 20ms distribution normal loss 0.2%
    apply_netem "$NODE3_CONTAINER" delay 65ms 20ms distribution normal loss 0.2%

    # --- Phase 1: Bidirectional writes (simulate cross-satellite telemetry) ---
    echo "[ISL] Phase 1: Writing data from satellite-1 (node-2)..."
    local key_a="sat-isl-a-$$"
    local key_b="sat-isl-b-$$"

    write_ops=$(measure_write_throughput "$NODE2_URL" "$key_a" "$WRITE_COUNT")
    echo "  Satellite-1 write throughput: ${write_ops} ops/s"

    echo "[ISL] Writing data from satellite-2 (node-3)..."
    local write_ops_b
    write_ops_b=$(measure_write_throughput "$NODE3_URL" "$key_b" "$WRITE_COUNT")
    echo "  Satellite-2 write throughput: ${write_ops_b} ops/s"

    # Wait for cross-satellite sync
    echo "[ISL] Waiting for inter-satellite delta sync..."
    sleep 8

    # --- Measure convergence ---
    echo "[ISL] Measuring convergence..."
    # satellite-1 data should reach ground station
    if ! measure_convergence "$WRITE_COUNT" "$key_a" "$NODE1_URL" "ground-station(key_a)"; then
        exit_code=1
    fi
    local conv_a=$CONVERGENCE_TIME_MS

    # satellite-2 data should reach ground station
    if ! measure_convergence "$WRITE_COUNT" "$key_b" "$NODE1_URL" "ground-station(key_b)"; then
        exit_code=1
    fi
    local conv_b=$CONVERGENCE_TIME_MS

    # Cross-satellite convergence
    if ! measure_convergence "$WRITE_COUNT" "$key_a" "$NODE3_URL" "satellite-2(key_a)"; then
        exit_code=1
    fi

    if ! measure_convergence "$WRITE_COUNT" "$key_b" "$NODE2_URL" "satellite-1(key_b)"; then
        exit_code=1
    fi

    # Read throughput
    echo "[ISL] Measuring read throughput..."
    read_ops=$(measure_read_throughput "$NODE1_URL" "$key_a" "$WRITE_COUNT")
    echo "  Read throughput: ${read_ops} ops/s"

    # --- Data integrity ---
    echo "[ISL] Verifying data integrity..."
    local data_ok=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        for key_check in "$key_a" "$key_b"; do
            local json val
            json=$(read_counter "$url" "$key_check")
            val=$(extract_value "$json")
            if [ "$val" = "$WRITE_COUNT" ]; then
                echo -e "  ${CLR_GREEN}[OK] ${name}: ${key_check} = ${val}${CLR_RESET}"
            else
                echo -e "  ${CLR_RED}[FAIL] ${name}: ${key_check} = ${val} (expected ${WRITE_COUNT})${CLR_RESET}"
                data_ok=false
                exit_code=1
            fi
        done
    done

    local avg_conv=$(( (conv_a + conv_b) / 2 ))
    echo ""
    echo "[ISL] Summary:"
    echo "  Avg convergence: ${avg_conv}ms"
    echo "  Write throughput: ${write_ops} / ${write_ops_b} ops/s (sat-1/sat-2)"
    echo "  Read throughput: ${read_ops} ops/s"
    echo "  Data integrity: ${data_ok}"

    local status="pass"
    [ "$exit_code" -ne 0 ] && status="fail"
    write_json_result "isl" "$status" "$avg_conv" "$write_ops" "$read_ops" "$data_ok"

    return "$exit_code"
}

# ======================================================================
# Scenario 5: Ground-to-LEO Handover
#   - Periodic link breaks every 90s simulating satellite pass
#   - Tests accumulation over multiple passes and GC stability
#   - Compressed timeline: 30s visible / 15s blackout, repeated 3 times
# ======================================================================
run_scenario_handover() {
    local key="sat-handover-$$"
    local exit_code=0
    local total_writes=0
    local write_ops=0
    local read_ops=0
    CONVERGENCE_TIME_MS=0

    trap 'remove_all_netem; remove_blackout "$NODE2_CONTAINER" 2>/dev/null || true' RETURN

    # Apply baseline LEO conditions to satellite node
    echo "[HANDOVER] Applying baseline LEO conditions to satellite (node-2)..."
    apply_netem "$NODE2_CONTAINER" delay 30ms 10ms distribution normal loss 2%

    local pass_count=3
    local visible_secs=15
    local blackout_secs=10
    local writes_per_pass=$WRITE_COUNT

    echo "[HANDOVER] Simulating ${pass_count} satellite passes:"
    echo "  Visible window: ${visible_secs}s"
    echo "  Blackout: ${blackout_secs}s"
    echo "  Writes per pass: ${writes_per_pass}"
    echo ""

    for pass in $(seq 1 "$pass_count"); do
        echo "--- Pass ${pass}/${pass_count} ---"

        # --- Visible window: satellite writes telemetry ---
        echo "[HANDOVER] Pass ${pass}: Satellite visible - writing telemetry..."
        write_counter_key "$NODE2_URL" "$key" "$writes_per_pass"
        total_writes=$(( total_writes + writes_per_pass ))
        echo "  Wrote ${writes_per_pass} increments (total: ${total_writes})"

        # Let sync happen during visible window
        echo "[HANDOVER] Syncing during visible window (${visible_secs}s)..."
        sleep "$visible_secs"

        # Check ground station has received data
        local json val
        json=$(read_counter "$NODE1_URL" "$key")
        val=$(extract_value "$json")
        echo "  Ground station value after pass ${pass}: ${val} (expected: ${total_writes})"

        if [ "$pass" -lt "$pass_count" ]; then
            # --- Blackout: satellite goes behind Earth ---
            echo "[HANDOVER] Pass ${pass}: Satellite going dark..."
            apply_blackout "$NODE2_CONTAINER"

            # Ground station writes during blackout (control commands)
            local cmd_key="sat-handover-cmd-$$"
            echo "[HANDOVER] Ground station sending control commands during blackout..."
            write_counter_key "$NODE1_URL" "$cmd_key" 3

            sleep "$blackout_secs"

            # --- Restore ---
            echo "[HANDOVER] Pass ${pass}: Satellite contact restored."
            remove_blackout "$NODE2_CONTAINER"
            sleep 3
        fi

        echo ""
    done

    # --- Measure final write throughput ---
    echo "[HANDOVER] Measuring sustained write throughput..."
    local extra_key="sat-handover-perf-$$"
    write_ops=$(measure_write_throughput "$NODE2_URL" "$extra_key" "$WRITE_COUNT")
    echo "  Write throughput: ${write_ops} ops/s"

    # --- Final convergence check ---
    echo "[HANDOVER] Waiting for final convergence..."
    sleep 10

    echo "[HANDOVER] Measuring final convergence..."
    if ! measure_convergence "$total_writes" "$key" "$NODE1_URL" "ground-station"; then
        exit_code=1
    fi
    local conv=$CONVERGENCE_TIME_MS

    if ! measure_convergence "$total_writes" "$key" "$NODE3_URL" "node-3"; then
        exit_code=1
    fi

    # Read throughput
    echo "[HANDOVER] Measuring read throughput..."
    read_ops=$(measure_read_throughput "$NODE1_URL" "$key" "$WRITE_COUNT")
    echo "  Read throughput: ${read_ops} ops/s"

    # --- Data integrity ---
    echo "[HANDOVER] Verifying data integrity across all nodes..."
    local data_ok=true
    for pair in "node-1:${NODE1_URL}" "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local json val
        json=$(read_counter "$url" "$key")
        val=$(extract_value "$json")
        if [ "$val" = "$total_writes" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name}: ${key} = ${val}${CLR_RESET}"
        else
            echo -e "  ${CLR_RED}[FAIL] ${name}: ${key} = ${val} (expected ${total_writes})${CLR_RESET}"
            data_ok=false
            exit_code=1
        fi
    done

    echo ""
    echo "[HANDOVER] Summary:"
    echo "  Passes completed: ${pass_count}"
    echo "  Total writes: ${total_writes}"
    echo "  Final convergence: ${conv}ms"
    echo "  Write throughput: ${write_ops} ops/s"
    echo "  Read throughput: ${read_ops} ops/s"
    echo "  Data integrity: ${data_ok}"

    local status="pass"
    [ "$exit_code" -ne 0 ] && status="fail"
    write_json_result "handover" "$status" "$conv" "$write_ops" "$read_ops" "$data_ok"

    return "$exit_code"
}

# ======================================================================
# Main execution
# ======================================================================
separator
echo -e "${CLR_BOLD}AsteroidDB Satellite Constellation Scenario Tests${CLR_RESET}"
separator
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Start cluster
echo "[satellite] Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
wait_for_cluster
echo ""

# Ensure tc is available on all nodes upfront
for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
    ensure_tc "$container"
done

# Determine which scenarios to run
SCENARIOS_TO_RUN=()
if [ -n "$RUN_SCENARIO" ]; then
    SCENARIOS_TO_RUN=("$RUN_SCENARIO")
else
    SCENARIOS_TO_RUN=("leo" "meo" "geo" "isl" "handover")
fi

TOTAL_SCENARIOS=${#SCENARIOS_TO_RUN[@]}
CURRENT=0

for scenario in "${SCENARIOS_TO_RUN[@]}"; do
    CURRENT=$(( CURRENT + 1 ))

    # Reset netem between scenarios
    remove_all_netem 2>/dev/null || true
    for container in "$NODE1_CONTAINER" "$NODE2_CONTAINER" "$NODE3_CONTAINER"; do
        remove_blackout "$container" 2>/dev/null || true
    done

    separator
    case "$scenario" in
        leo)
            echo -e "${CLR_BOLD}Scenario ${CURRENT}/${TOTAL_SCENARIOS}: LEO (Low Earth Orbit)${CLR_RESET}"
            echo "  RTT: 20-40ms | Loss: 1-5% | Intermittent connectivity"
            ;;
        meo)
            echo -e "${CLR_BOLD}Scenario ${CURRENT}/${TOTAL_SCENARIOS}: MEO (Medium Earth Orbit)${CLR_RESET}"
            echo "  RTT: 100-150ms | Loss: 0.5% | Stable connectivity"
            ;;
        geo)
            echo -e "${CLR_BOLD}Scenario ${CURRENT}/${TOTAL_SCENARIOS}: GEO (Geostationary Orbit)${CLR_RESET}"
            echo "  RTT: 500-600ms | Loss: 0.1% | Stable connectivity"
            ;;
        isl)
            echo -e "${CLR_BOLD}Scenario ${CURRENT}/${TOTAL_SCENARIOS}: ISL (Inter-Satellite Link)${CLR_RESET}"
            echo "  RTT: 50-80ms | Jitter: +/-20ms | Variable conditions"
            ;;
        handover)
            echo -e "${CLR_BOLD}Scenario ${CURRENT}/${TOTAL_SCENARIOS}: Ground-to-LEO Handover${CLR_RESET}"
            echo "  Periodic link breaks simulating satellite pass"
            ;;
        *)
            echo -e "${CLR_RED}Unknown scenario: ${scenario}${CLR_RESET}"
            FAIL_COUNT=$(( FAIL_COUNT + 1 ))
            continue
            ;;
    esac
    sub_separator

    S_START=$(date +%s)
    S_EXIT=0
    case "$scenario" in
        leo)      run_scenario_leo      || S_EXIT=$? ;;
        meo)      run_scenario_meo      || S_EXIT=$? ;;
        geo)      run_scenario_geo      || S_EXIT=$? ;;
        isl)      run_scenario_isl      || S_EXIT=$? ;;
        handover) run_scenario_handover || S_EXIT=$? ;;
    esac
    scenario_result "$scenario" "$S_EXIT" "$S_START"
    echo ""
done

# ======================================================================
# Summary
# ======================================================================
TOTAL_END=$(date +%s)
TOTAL_DURATION=$(( TOTAL_END - TOTAL_START ))

separator
echo -e "${CLR_BOLD}Satellite Scenario Summary${CLR_RESET}"
sub_separator
echo "  Passed: ${PASS_COUNT}"
echo "  Failed: ${FAIL_COUNT}"
echo "  Total time: ${TOTAL_DURATION}s"
echo "  Results: ${RESULTS_DIR}/"
separator

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${CLR_RED}Some scenarios failed.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}All satellite constellation scenarios passed.${CLR_RESET}"
exit 0
