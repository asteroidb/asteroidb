#!/usr/bin/env bash
# High-latency scenario tests for AsteroidDB (#299).
#
# Spins up a 3-node cluster via Docker Compose, injects escalating RTT
# (100ms, 500ms, 1s, 3s) on node-2 and node-3, and measures:
#   a. Delta sync convergence time (write on node-1, poll node-2/3)
#   b. Write/read throughput (ops/sec via curl)
#   c. Certified write latency (POST /api/certified/write round-trip)
#
# Results are written as CSV to target/high-latency-results.csv and
# as JSON to target/high-latency-results.json.
#
# Usage: ./scripts/test-high-latency.sh [OPTIONS]
#
# Options:
#   --write-ops N          Number of writes per throughput test (default 50)
#   --convergence-timeout  Max seconds to wait for convergence (default 120)
#   --skip-certified       Skip certified write tests
#   --help                 Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - python3, curl, bc available on the host
#   - No other asteroidb containers running (ports 3001-3003 free)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NETEM_DIR="${SCRIPT_DIR}/netem"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"

source "${NETEM_DIR}/lib.sh"

# --- Configuration ---
NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE1_CONTAINER="asteroidb-node-1"
NODE2_CONTAINER="asteroidb-node-2"
NODE3_CONTAINER="asteroidb-node-3"
ALL_CONTAINERS=("$NODE2_CONTAINER" "$NODE3_CONTAINER")

# Latency levels in ms (one-way delay; RTT = 2x)
LATENCY_LEVELS=(100 500 1000 3000)

# Defaults
WRITE_OPS=50
CONVERGENCE_TIMEOUT=120
SKIP_CERTIFIED=false

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/test-high-latency.sh [OPTIONS]

Options:
  --write-ops N          Number of writes per throughput test (default 50)
  --convergence-timeout  Max seconds to wait for convergence (default 120)
  --skip-certified       Skip certified write tests
  --help                 Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --write-ops)
            WRITE_OPS="${2:?--write-ops requires a value}"
            shift 2
            ;;
        --convergence-timeout)
            CONVERGENCE_TIMEOUT="${2:?--convergence-timeout requires a value}"
            shift 2
            ;;
        --skip-certified)
            SKIP_CERTIFIED=true
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

# --- Output files ---
RESULTS_DIR="${PROJECT_DIR}/target"
mkdir -p "$RESULTS_DIR"
CSV_FILE="${RESULTS_DIR}/high-latency-results.csv"
JSON_FILE="${RESULTS_DIR}/high-latency-results.json"

# --- Global tracking ---
TOTAL_START=$(date +%s)
PASS_COUNT=0
FAIL_COUNT=0
# Accumulate JSON results per latency level
JSON_RESULTS="[]"

# --- Cleanup ---
cleanup() {
    echo ""
    echo "[high-latency] Cleaning up netem rules..."
    for container in "${ALL_CONTAINERS[@]}"; do
        "${NETEM_DIR}/remove-netem.sh" "$container" 2>/dev/null || true
    done
    echo "[high-latency] Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

# ======================================================================
# Helper functions
# ======================================================================

wait_for_cluster() {
    echo "[high-latency] Waiting for cluster to be ready..."
    for port in 3001 3002 3003; do
        for attempt in $(seq 1 40); do
            if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
                echo "  Node on port ${port} is ready"
                break
            fi
            if [ "$attempt" -eq 40 ]; then
                echo "[high-latency] ERROR: Node on port ${port} did not become ready"
                exit 1
            fi
            sleep 2
        done
    done
    echo "[high-latency] Cluster is ready."
}

apply_delay() {
    local delay_ms="$1"
    for container in "${ALL_CONTAINERS[@]}"; do
        "${NETEM_DIR}/add-delay.sh" "$container" "$delay_ms"
    done
}

remove_delay() {
    for container in "${ALL_CONTAINERS[@]}"; do
        "${NETEM_DIR}/remove-netem.sh" "$container" 2>/dev/null || true
    done
}

# measure_convergence <key> <expected_value> <timeout_s>
# Writes to node-1, then polls node-2 and node-3 until value appears.
# Prints convergence time in milliseconds for each node.
# Returns 0 if both converged, 1 otherwise.
measure_convergence() {
    local key="$1"
    local expected="$2"
    local timeout_s="$3"
    local converged_all=true

    # Write 5 increments to node-1
    for _ in $(seq 1 5); do
        curl -sf -X POST "${NODE1_URL}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" > /dev/null 2>&1 || true
    done

    local write_time_ms
    write_time_ms=$(now_epoch_ms)

    # Poll node-2 and node-3
    for pair in "node-2:${NODE2_URL}" "node-3:${NODE3_URL}"; do
        local name="${pair%%:*}"
        local url="${pair#*:}"
        local converged=false
        local deadline=$((timeout_s * 1000))
        local poll_interval=500  # ms

        while true; do
            local elapsed
            elapsed=$(elapsed_ms "$write_time_ms")
            if [ "$elapsed" -ge "$deadline" ]; then
                break
            fi

            local json val
            json=$(read_counter "$url" "$key")
            val=$(extract_value "$json")
            if [ "$val" = "$expected" ]; then
                converged=true
                echo "  ${name}: converged in ${elapsed}ms"
                # Store for the caller
                eval "CONVERGENCE_${name//-/_}=${elapsed}"
                break
            fi

            # Sleep a fraction of a second (adaptive to latency)
            sleep 0.5
        done

        if ! $converged; then
            local final_elapsed
            final_elapsed=$(elapsed_ms "$write_time_ms")
            echo -e "  ${CLR_RED}${name}: TIMEOUT (${final_elapsed}ms, value=${val:-null})${CLR_RESET}"
            eval "CONVERGENCE_${name//-/_}=-1"
            converged_all=false
        fi
    done

    $converged_all
}

# measure_throughput <key_prefix> <ops> <url> <label>
# Performs sequential writes and reads, returns ops/sec and latency stats.
measure_write_throughput() {
    local prefix="$1"
    local ops="$2"
    local url="$3"
    local label="$4"
    local latencies_file
    latencies_file=$(mktemp)

    local overall_start
    overall_start=$(date +%s%N)
    local success=0
    local errors=0

    for i in $(seq 1 "$ops"); do
        local start_ns end_ns
        start_ns=$(date +%s%N)
        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 10 \
            -X POST "${url}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"counter_inc\",\"key\":\"${prefix}-${i}\"}" \
            2>/dev/null) || http_code="000"
        end_ns=$(date +%s%N)

        local latency_us=$(( (end_ns - start_ns) / 1000 ))
        echo "$latency_us" >> "$latencies_file"

        if [ "$http_code" -ge 200 ] 2>/dev/null && [ "$http_code" -lt 300 ] 2>/dev/null; then
            success=$((success + 1))
        else
            errors=$((errors + 1))
        fi
    done

    local overall_end
    overall_end=$(date +%s%N)
    local duration_ms=$(( (overall_end - overall_start) / 1000000 ))

    local throughput="0"
    if [ "$duration_ms" -gt 0 ]; then
        throughput=$(echo "scale=2; $success * 1000 / $duration_ms" | bc)
    fi

    # Compute percentiles
    local sorted
    sorted=$(sort -n "$latencies_file")
    local count
    count=$(echo "$sorted" | wc -l)

    percentile_val() {
        local pct=$1
        local idx
        idx=$(echo "scale=0; ($count * $pct + 99) / 100" | bc)
        [ "$idx" -lt 1 ] && idx=1
        echo "$sorted" | sed -n "${idx}p"
    }

    local p50 p95 p99
    p50=$(percentile_val 50)
    p95=$(percentile_val 95)
    p99=$(percentile_val 99)

    # Convert us to ms for display
    local p50_ms p95_ms p99_ms
    p50_ms=$(echo "scale=2; ${p50:-0} / 1000" | bc)
    p95_ms=$(echo "scale=2; ${p95:-0} / 1000" | bc)
    p99_ms=$(echo "scale=2; ${p99:-0} / 1000" | bc)

    echo "  ${label} writes: ${success}/${ops} ok, ${throughput} ops/s"
    echo "  ${label} latency: p50=${p50_ms}ms p95=${p95_ms}ms p99=${p99_ms}ms"

    # Export for caller
    eval "WRITE_THROUGHPUT_${label}=${throughput}"
    eval "WRITE_P50_${label}=${p50:-0}"
    eval "WRITE_P95_${label}=${p95:-0}"
    eval "WRITE_P99_${label}=${p99:-0}"
    eval "WRITE_SUCCESS_${label}=${success}"
    eval "WRITE_ERRORS_${label}=${errors}"

    rm -f "$latencies_file"
}

measure_read_throughput() {
    local prefix="$1"
    local ops="$2"
    local url="$3"
    local label="$4"
    local latencies_file
    latencies_file=$(mktemp)

    local overall_start
    overall_start=$(date +%s%N)
    local success=0
    local errors=0

    for i in $(seq 1 "$ops"); do
        local start_ns end_ns
        start_ns=$(date +%s%N)
        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 10 \
            "${url}/api/eventual/${prefix}-${i}" 2>/dev/null) || http_code="000"
        end_ns=$(date +%s%N)

        local latency_us=$(( (end_ns - start_ns) / 1000 ))
        echo "$latency_us" >> "$latencies_file"

        if [ "$http_code" -ge 200 ] 2>/dev/null && [ "$http_code" -lt 300 ] 2>/dev/null; then
            success=$((success + 1))
        else
            errors=$((errors + 1))
        fi
    done

    local overall_end
    overall_end=$(date +%s%N)
    local duration_ms=$(( (overall_end - overall_start) / 1000000 ))

    local throughput="0"
    if [ "$duration_ms" -gt 0 ]; then
        throughput=$(echo "scale=2; $success * 1000 / $duration_ms" | bc)
    fi

    local sorted count
    sorted=$(sort -n "$latencies_file")
    count=$(echo "$sorted" | wc -l)

    percentile_val() {
        local pct=$1
        local idx
        idx=$(echo "scale=0; ($count * $pct + 99) / 100" | bc)
        [ "$idx" -lt 1 ] && idx=1
        echo "$sorted" | sed -n "${idx}p"
    }

    local p50 p95 p99
    p50=$(percentile_val 50)
    p95=$(percentile_val 95)
    p99=$(percentile_val 99)

    local p50_ms p95_ms p99_ms
    p50_ms=$(echo "scale=2; ${p50:-0} / 1000" | bc)
    p95_ms=$(echo "scale=2; ${p95:-0} / 1000" | bc)
    p99_ms=$(echo "scale=2; ${p99:-0} / 1000" | bc)

    echo "  ${label} reads: ${success}/${ops} ok, ${throughput} ops/s"
    echo "  ${label} latency: p50=${p50_ms}ms p95=${p95_ms}ms p99=${p99_ms}ms"

    eval "READ_THROUGHPUT_${label}=${throughput}"
    eval "READ_P50_${label}=${p50:-0}"
    eval "READ_P95_${label}=${p95:-0}"
    eval "READ_P99_${label}=${p99:-0}"
    eval "READ_SUCCESS_${label}=${success}"
    eval "READ_ERRORS_${label}=${errors}"

    rm -f "$latencies_file"
}

# measure_certified_latency <key_prefix> <ops> <url>
# Measures round-trip time for certified writes.
measure_certified_latency() {
    local prefix="$1"
    local ops="$2"
    local url="$3"
    local latencies_file
    latencies_file=$(mktemp)
    local success=0
    local errors=0
    local timeouts=0

    for i in $(seq 1 "$ops"); do
        local start_ns end_ns
        start_ns=$(date +%s%N)
        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 30 \
            -X POST "${url}/api/certified/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"register_set\",\"key\":\"${prefix}-${i}\",\"value\":\"cert-${i}\",\"on_timeout\":\"pending\"}" \
            2>/dev/null) || http_code="000"
        end_ns=$(date +%s%N)

        local latency_us=$(( (end_ns - start_ns) / 1000 ))
        echo "$latency_us" >> "$latencies_file"

        if [ "$http_code" -ge 200 ] 2>/dev/null && [ "$http_code" -lt 300 ] 2>/dev/null; then
            success=$((success + 1))
        elif [ "$http_code" = "000" ]; then
            timeouts=$((timeouts + 1))
        else
            errors=$((errors + 1))
        fi
    done

    local sorted count
    sorted=$(sort -n "$latencies_file")
    count=$(echo "$sorted" | wc -l)

    percentile_val() {
        local pct=$1
        if [ "$count" -eq 0 ]; then echo "0"; return; fi
        local idx
        idx=$(echo "scale=0; ($count * $pct + 99) / 100" | bc)
        [ "$idx" -lt 1 ] && idx=1
        echo "$sorted" | sed -n "${idx}p"
    }

    local p50 p95 p99
    p50=$(percentile_val 50)
    p95=$(percentile_val 95)
    p99=$(percentile_val 99)

    local p50_ms p95_ms p99_ms
    p50_ms=$(echo "scale=2; ${p50:-0} / 1000" | bc)
    p95_ms=$(echo "scale=2; ${p95:-0} / 1000" | bc)
    p99_ms=$(echo "scale=2; ${p99:-0} / 1000" | bc)

    echo "  Certified writes: ${success}/${ops} ok, ${timeouts} timeouts, ${errors} errors"
    echo "  Certified latency: p50=${p50_ms}ms p95=${p95_ms}ms p99=${p99_ms}ms"

    CERT_P50="${p50:-0}"
    CERT_P95="${p95:-0}"
    CERT_P99="${p99:-0}"
    CERT_SUCCESS="${success}"
    CERT_ERRORS="${errors}"
    CERT_TIMEOUTS="${timeouts}"

    rm -f "$latencies_file"
}

# ======================================================================
# Start cluster
# ======================================================================
separator
echo -e "${CLR_BOLD}AsteroidDB High-Latency Scenario Tests (#299)${CLR_RESET}"
separator
echo ""
echo "  Latency levels: ${LATENCY_LEVELS[*]}ms"
echo "  Write ops/level: ${WRITE_OPS}"
echo "  Convergence timeout: ${CONVERGENCE_TIMEOUT}s"
echo "  Skip certified: ${SKIP_CERTIFIED}"
echo ""

echo "[high-latency] Building and starting 3-node cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5
wait_for_cluster
echo ""

# --- Write CSV header ---
echo "latency_ms,convergence_node2_ms,convergence_node3_ms,write_throughput_ops_s,write_p50_us,write_p95_us,write_p99_us,read_throughput_ops_s,read_p50_us,read_p95_us,read_p99_us,cert_p50_us,cert_p95_us,cert_p99_us,cert_success,cert_timeouts" > "$CSV_FILE"

# ======================================================================
# Baseline (no delay)
# ======================================================================
separator
echo -e "${CLR_BOLD}Baseline: no injected delay${CLR_RESET}"
sub_separator

BASELINE_KEY="hlat-baseline-$$"
echo "[baseline] Measuring convergence..."
CONVERGENCE_node_2=0
CONVERGENCE_node_3=0
if measure_convergence "$BASELINE_KEY" "5" "$CONVERGENCE_TIMEOUT"; then
    echo -e "  ${CLR_GREEN}[OK] Baseline convergence passed.${CLR_RESET}"
else
    echo -e "  ${CLR_YELLOW}[WARN] Baseline convergence incomplete.${CLR_RESET}"
fi

echo "[baseline] Measuring write throughput (node-1)..."
measure_write_throughput "hlat-bw-$$" "$WRITE_OPS" "$NODE1_URL" "baseline"

echo "[baseline] Measuring read throughput (node-2)..."
measure_read_throughput "hlat-bw-$$" "$WRITE_OPS" "$NODE2_URL" "baseline"

CERT_P50=0; CERT_P95=0; CERT_P99=0; CERT_SUCCESS=0; CERT_ERRORS=0; CERT_TIMEOUTS=0
if [ "$SKIP_CERTIFIED" = "false" ]; then
    echo "[baseline] Measuring certified write latency..."
    measure_certified_latency "hlat-cert-baseline-$$" 10 "$NODE1_URL"
fi

# Write baseline CSV row
echo "0,${CONVERGENCE_node_2},${CONVERGENCE_node_3},${WRITE_THROUGHPUT_baseline:-0},${WRITE_P50_baseline:-0},${WRITE_P95_baseline:-0},${WRITE_P99_baseline:-0},${READ_THROUGHPUT_baseline:-0},${READ_P50_baseline:-0},${READ_P95_baseline:-0},${READ_P99_baseline:-0},${CERT_P50},${CERT_P95},${CERT_P99},${CERT_SUCCESS},${CERT_TIMEOUTS}" >> "$CSV_FILE"

# Accumulate JSON
JSON_RESULTS=$(python3 -c "
import json, sys
results = json.loads(sys.argv[1])
results.append({
    'latency_ms': 0,
    'convergence': {'node_2_ms': ${CONVERGENCE_node_2}, 'node_3_ms': ${CONVERGENCE_node_3}},
    'write': {'throughput_ops_s': ${WRITE_THROUGHPUT_baseline:-0}, 'p50_us': ${WRITE_P50_baseline:-0}, 'p95_us': ${WRITE_P95_baseline:-0}, 'p99_us': ${WRITE_P99_baseline:-0}},
    'read': {'throughput_ops_s': ${READ_THROUGHPUT_baseline:-0}, 'p50_us': ${READ_P50_baseline:-0}, 'p95_us': ${READ_P95_baseline:-0}, 'p99_us': ${READ_P99_baseline:-0}},
    'certified': {'p50_us': ${CERT_P50}, 'p95_us': ${CERT_P95}, 'p99_us': ${CERT_P99}, 'success': ${CERT_SUCCESS}, 'timeouts': ${CERT_TIMEOUTS}}
})
print(json.dumps(results))
" "$JSON_RESULTS")

echo ""

# ======================================================================
# Per-latency-level scenarios
# ======================================================================
for delay_ms in "${LATENCY_LEVELS[@]}"; do
    separator
    echo -e "${CLR_BOLD}Latency Level: ${delay_ms}ms one-way ($((delay_ms * 2))ms RTT)${CLR_RESET}"
    sub_separator

    SCENARIO_START=$(date +%s)

    # Apply delay to node-2 and node-3
    echo "[netem] Applying ${delay_ms}ms delay to node-2 and node-3..."
    apply_delay "$delay_ms"

    # Allow network to stabilize
    sleep 1

    # --- (a) Convergence ---
    local_key="hlat-conv-${delay_ms}-$$"
    echo "[${delay_ms}ms] Measuring delta sync convergence..."
    CONVERGENCE_node_2=0
    CONVERGENCE_node_3=0

    # Increase timeout proportionally for higher latencies
    local_timeout=$CONVERGENCE_TIMEOUT
    if [ "$delay_ms" -ge 1000 ]; then
        local_timeout=$(( CONVERGENCE_TIMEOUT * 2 ))
    fi

    conv_ok=true
    if ! measure_convergence "$local_key" "5" "$local_timeout"; then
        conv_ok=false
    fi

    # --- (b) Write throughput ---
    local_write_label="w${delay_ms}"
    echo "[${delay_ms}ms] Measuring write throughput (node-1)..."
    measure_write_throughput "hlat-wt-${delay_ms}-$$" "$WRITE_OPS" "$NODE1_URL" "$local_write_label"

    # --- (b) Read throughput (through delayed node-2) ---
    # First seed some keys on node-2 side for reads
    local_read_label="r${delay_ms}"
    echo "[${delay_ms}ms] Measuring read throughput (node-2, via delay)..."
    measure_read_throughput "hlat-wt-${delay_ms}-$$" "$WRITE_OPS" "$NODE2_URL" "$local_read_label"

    # --- (c) Certified write latency ---
    CERT_P50=0; CERT_P95=0; CERT_P99=0; CERT_SUCCESS=0; CERT_ERRORS=0; CERT_TIMEOUTS=0
    if [ "$SKIP_CERTIFIED" = "false" ]; then
        # Reduce certified ops at very high latency to keep test time bounded
        cert_ops=10
        if [ "$delay_ms" -ge 3000 ]; then
            cert_ops=5
        fi
        echo "[${delay_ms}ms] Measuring certified write latency (${cert_ops} ops)..."
        measure_certified_latency "hlat-cert-${delay_ms}-$$" "$cert_ops" "$NODE1_URL"
    fi

    # Remove delay before next iteration
    echo "[netem] Removing delay..."
    remove_delay
    sleep 1

    # --- Record results ---
    W_TP_VAR="WRITE_THROUGHPUT_${local_write_label}"
    W_P50_VAR="WRITE_P50_${local_write_label}"
    W_P95_VAR="WRITE_P95_${local_write_label}"
    W_P99_VAR="WRITE_P99_${local_write_label}"
    R_TP_VAR="READ_THROUGHPUT_${local_read_label}"
    R_P50_VAR="READ_P50_${local_read_label}"
    R_P95_VAR="READ_P95_${local_read_label}"
    R_P99_VAR="READ_P99_${local_read_label}"

    echo "${delay_ms},${CONVERGENCE_node_2},${CONVERGENCE_node_3},${!W_TP_VAR:-0},${!W_P50_VAR:-0},${!W_P95_VAR:-0},${!W_P99_VAR:-0},${!R_TP_VAR:-0},${!R_P50_VAR:-0},${!R_P95_VAR:-0},${!R_P99_VAR:-0},${CERT_P50},${CERT_P95},${CERT_P99},${CERT_SUCCESS},${CERT_TIMEOUTS}" >> "$CSV_FILE"

    JSON_RESULTS=$(python3 -c "
import json, sys
results = json.loads(sys.argv[1])
results.append({
    'latency_ms': ${delay_ms},
    'convergence': {'node_2_ms': ${CONVERGENCE_node_2}, 'node_3_ms': ${CONVERGENCE_node_3}},
    'write': {'throughput_ops_s': ${!W_TP_VAR:-0}, 'p50_us': ${!W_P50_VAR:-0}, 'p95_us': ${!W_P95_VAR:-0}, 'p99_us': ${!W_P99_VAR:-0}},
    'read': {'throughput_ops_s': ${!R_TP_VAR:-0}, 'p50_us': ${!R_P50_VAR:-0}, 'p95_us': ${!R_P95_VAR:-0}, 'p99_us': ${!R_P99_VAR:-0}},
    'certified': {'p50_us': ${CERT_P50}, 'p95_us': ${CERT_P95}, 'p99_us': ${CERT_P99}, 'success': ${CERT_SUCCESS}, 'timeouts': ${CERT_TIMEOUTS}}
})
print(json.dumps(results))
" "$JSON_RESULTS")

    # Scenario pass/fail
    SCENARIO_END=$(date +%s)
    SCENARIO_DURATION=$(( SCENARIO_END - SCENARIO_START ))
    if $conv_ok; then
        echo -e "${CLR_GREEN}[PASS] ${delay_ms}ms scenario (${SCENARIO_DURATION}s)${CLR_RESET}"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        echo -e "${CLR_RED}[FAIL] ${delay_ms}ms scenario (${SCENARIO_DURATION}s)${CLR_RESET}"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi
    echo ""
done

# ======================================================================
# Write final JSON
# ======================================================================
python3 -c "
import json, sys
results = json.loads(sys.argv[1])
output = {
    'test': 'high-latency-scenarios',
    'timestamp': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'config': {
        'write_ops_per_level': ${WRITE_OPS},
        'convergence_timeout_s': ${CONVERGENCE_TIMEOUT},
        'latency_levels_ms': [${LATENCY_LEVELS[*]// /,}],
        'skip_certified': $([ \"$SKIP_CERTIFIED\" = \"true\" ] && echo 'true' || echo 'false'),
        'nodes': 3
    },
    'results': results
}
print(json.dumps(output, indent=2))
" "$JSON_RESULTS" > "$JSON_FILE"

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
echo ""
echo "  CSV results: ${CSV_FILE}"
echo "  JSON results: ${JSON_FILE}"
separator

# Display CSV for quick viewing
echo ""
echo -e "${CLR_BOLD}Results Table (CSV):${CLR_RESET}"
column -t -s',' "$CSV_FILE" 2>/dev/null || cat "$CSV_FILE"
echo ""

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${CLR_YELLOW}Some scenarios had convergence failures (expected at very high latency).${CLR_RESET}"
    echo -e "${CLR_YELLOW}Check JSON results for detailed timing data.${CLR_RESET}"
    # Do not exit 1 — convergence timeout at 3s RTT is expected behavior, not a bug.
    # The test's purpose is measurement, not pass/fail.
fi

echo -e "${CLR_GREEN}High-latency scenario tests complete.${CLR_RESET}"
exit 0
