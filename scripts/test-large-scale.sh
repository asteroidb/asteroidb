#!/usr/bin/env bash
# test-large-scale.sh — Large-scale node cluster test for AsteroidDB.
#
# Spins up a 7-node cluster, runs sustained write load, monitors resource
# usage over time, checks for memory leaks, verifies convergence after
# load stops, and tests node join/leave during sustained load.
#
# Outputs time-series metrics as CSV to target/large-scale-metrics.csv.
#
# Usage:
#   ./scripts/test-large-scale.sh [OPTIONS]
#
# Options:
#   --duration SECONDS   Sustained load duration (default: 300 = 5 min)
#   --concurrency N      Number of concurrent writers (default: 10)
#   --sample-interval S  Seconds between metric samples (default: 10)
#   --help               Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - curl, python3 available on the host
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NETEM_DIR="${SCRIPT_DIR}/netem"

# Source shared helpers if available.
if [ -f "${NETEM_DIR}/lib.sh" ]; then
    source "${NETEM_DIR}/lib.sh"
else
    # Minimal fallback definitions.
    CLR_GREEN="" CLR_RED="" CLR_YELLOW="" CLR_BOLD="" CLR_RESET=""
    separator() { echo "======================================================================"; }
    sub_separator() { echo "----------------------------------------------------------------------"; }
    log_step() { local n="$1"; shift; separator; echo "STEP ${n}: $*"; sub_separator; }
fi

# --- Defaults ---
DURATION=300
CONCURRENCY=10
SAMPLE_INTERVAL=10

COMPOSE_FILE="$PROJECT_DIR/docker-compose.scale.yml"
NODE_COUNT=7
HEALTH_TIMEOUT=120

# Ports mapped from docker-compose.scale.yml.
declare -a NODE_PORTS
for i in $(seq 1 $NODE_COUNT); do
    NODE_PORTS+=("300${i}")
done

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/test-large-scale.sh [OPTIONS]

Options:
  --duration SECONDS   Sustained load duration (default: 300 = 5 min)
  --concurrency N      Number of concurrent writers (default: 10)
  --sample-interval S  Seconds between metric samples (default: 10)
  --help               Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)       DURATION="${2:?--duration requires a value}"; shift 2 ;;
        --concurrency)    CONCURRENCY="${2:?--concurrency requires a value}"; shift 2 ;;
        --sample-interval) SAMPLE_INTERVAL="${2:?--sample-interval requires a value}"; shift 2 ;;
        --help)           usage; exit 0 ;;
        *)                echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# --- Output files ---
RESULTS_DIR="$PROJECT_DIR/target"
mkdir -p "$RESULTS_DIR"
METRICS_CSV="$RESULTS_DIR/large-scale-metrics.csv"
RESULTS_JSON="$RESULTS_DIR/large-scale-results.json"

# --- Token ---
export ASTEROIDB_INTERNAL_TOKEN="${ASTEROIDB_INTERNAL_TOKEN:-scale-test-$(date +%s)}"

# --- Cleanup trap ---
cleanup() {
    echo ""
    echo "[scale-test] Stopping background workers..."
    # Kill any remaining background jobs.
    jobs -p 2>/dev/null | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true
    echo "[scale-test] Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down --timeout 10 --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# --- Helper functions ---

get_container_memory_mb() {
    local container="$1"
    # docker stats gives memory usage; parse the MiB value.
    local mem
    mem=$(docker stats --no-stream --format "{{.MemUsage}}" "$container" 2>/dev/null | awk -F'/' '{gsub(/[^0-9.]/, "", $1); print $1}')
    echo "${mem:-0}"
}

get_container_cpu_pct() {
    local container="$1"
    local cpu
    cpu=$(docker stats --no-stream --format "{{.CPUPerc}}" "$container" 2>/dev/null | tr -d '%')
    echo "${cpu:-0}"
}

write_eventual_key() {
    local port="$1"
    local key="$2"
    local value="$3"
    curl -sf -X POST "http://localhost:${port}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"register_set\",\"key\":\"${key}\",\"value\":\"${value}\"}" \
        -o /dev/null --max-time 10 2>/dev/null || true
}

write_counter_inc() {
    local port="$1"
    local key="$2"
    curl -sf -X POST "http://localhost:${port}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" \
        -o /dev/null --max-time 10 2>/dev/null || true
}

read_eventual_key() {
    local port="$1"
    local key="$2"
    curl -sf --max-time 5 "http://localhost:${port}/api/eventual/${key}" 2>/dev/null || echo '{"value":null}'
}

check_node_health() {
    local port="$1"
    curl -sf --max-time 3 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1
}

# =====================================================================
# Step 1: Start the 7-node cluster
# =====================================================================
separator
echo -e "${CLR_BOLD}AsteroidDB Large-Scale Test (${NODE_COUNT} nodes)${CLR_RESET}"
separator
echo ""
echo "  Duration:        ${DURATION}s"
echo "  Concurrency:     ${CONCURRENCY} writers"
echo "  Sample interval: ${SAMPLE_INTERVAL}s"
echo "  Nodes:           ${NODE_COUNT}"
echo ""

log_step 1 "Starting ${NODE_COUNT}-node cluster"
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5

echo "[scale-test] Waiting for all nodes to become healthy (timeout: ${HEALTH_TIMEOUT}s)..."
for port in "${NODE_PORTS[@]}"; do
    elapsed=0
    while [ "$elapsed" -lt "$HEALTH_TIMEOUT" ]; do
        if check_node_health "$port"; then
            echo "  node on port ${port}: UP"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    if [ "$elapsed" -ge "$HEALTH_TIMEOUT" ]; then
        echo -e "  ${CLR_RED}node on port ${port}: TIMEOUT after ${HEALTH_TIMEOUT}s${CLR_RESET}" >&2
        echo "Docker logs:"
        docker compose -f "$COMPOSE_FILE" logs --tail=30
        exit 1
    fi
done
echo -e "${CLR_GREEN}All ${NODE_COUNT} nodes are healthy.${CLR_RESET}"
echo ""

# =====================================================================
# Step 2: Collect baseline memory
# =====================================================================
log_step 2 "Collecting baseline memory metrics"

echo "timestamp_s,elapsed_s,node,memory_mb,cpu_pct" > "$METRICS_CSV"

collect_metrics() {
    local ts elapsed
    ts=$(date +%s)
    elapsed=$(( ts - TEST_START ))
    for i in $(seq 1 $NODE_COUNT); do
        local container="asteroidb-node-${i}"
        local mem cpu
        mem=$(get_container_memory_mb "$container")
        cpu=$(get_container_cpu_pct "$container")
        echo "${ts},${elapsed},node-${i},${mem},${cpu}" >> "$METRICS_CSV"
    done
}

TEST_START=$(date +%s)
collect_metrics
echo "  Baseline metrics recorded."

declare -A BASELINE_MEM
for i in $(seq 1 $NODE_COUNT); do
    container="asteroidb-node-${i}"
    BASELINE_MEM[$i]=$(get_container_memory_mb "$container")
    echo "  node-${i} baseline memory: ${BASELINE_MEM[$i]} MiB"
done
echo ""

# =====================================================================
# Step 3: Sustained write load with periodic metric sampling
# =====================================================================
log_step 3 "Sustained write load for ${DURATION}s with ${CONCURRENCY} workers"

WRITE_COUNT=0
WRITE_ERRORS=0
WRITE_COUNT_FILE=$(mktemp)
WRITE_ERROR_FILE=$(mktemp)
echo "0" > "$WRITE_COUNT_FILE"
echo "0" > "$WRITE_ERROR_FILE"

# Background writer function.
writer_loop() {
    local worker_id="$1"
    local end_time="$2"
    local count=0
    local errors=0
    while [ "$(date +%s)" -lt "$end_time" ]; do
        local port_idx=$(( (worker_id + count) % NODE_COUNT ))
        local port="${NODE_PORTS[$port_idx]}"
        local key="scale/w${worker_id}/k${count}"
        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
            -X POST "http://localhost:${port}/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"type\":\"register_set\",\"key\":\"${key}\",\"value\":\"v${count}\"}" \
            --max-time 10 2>/dev/null) || http_code="000"
        if [[ "$http_code" =~ ^2 ]]; then
            count=$((count + 1))
        else
            errors=$((errors + 1))
        fi
    done
    # Append counts atomically.
    flock "$WRITE_COUNT_FILE" bash -c "echo \$(( \$(cat '$WRITE_COUNT_FILE') + $count )) > '$WRITE_COUNT_FILE'"
    flock "$WRITE_ERROR_FILE" bash -c "echo \$(( \$(cat '$WRITE_ERROR_FILE') + $errors )) > '$WRITE_ERROR_FILE'"
}

END_TIME=$(( $(date +%s) + DURATION ))

# Launch writer workers.
WRITER_PIDS=()
for w in $(seq 0 $((CONCURRENCY - 1))); do
    writer_loop "$w" "$END_TIME" &
    WRITER_PIDS+=($!)
done

echo "  ${CONCURRENCY} writers launched. Sampling metrics every ${SAMPLE_INTERVAL}s..."

# Metric sampling loop (runs until duration ends).
while [ "$(date +%s)" -lt "$END_TIME" ]; do
    sleep "$SAMPLE_INTERVAL"
    collect_metrics
    elapsed=$(( $(date +%s) - TEST_START ))
    echo "  [${elapsed}s] metrics sampled"
done

# Wait for all writers to finish.
echo "  Waiting for writers to complete..."
for pid in "${WRITER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

WRITE_COUNT=$(cat "$WRITE_COUNT_FILE")
WRITE_ERRORS=$(cat "$WRITE_ERROR_FILE")
rm -f "$WRITE_COUNT_FILE" "$WRITE_ERROR_FILE"

# Collect final metrics.
collect_metrics

echo ""
echo "  Total writes:  ${WRITE_COUNT}"
echo "  Write errors:  ${WRITE_ERRORS}"
DURATION_ACTUAL=$(( $(date +%s) - TEST_START ))
if [ "$DURATION_ACTUAL" -gt 0 ]; then
    THROUGHPUT=$(( WRITE_COUNT / DURATION_ACTUAL ))
else
    THROUGHPUT=0
fi
echo "  Throughput:    ~${THROUGHPUT} ops/sec"
echo ""

# =====================================================================
# Step 4: Memory leak detection
# =====================================================================
log_step 4 "Memory leak analysis"

LEAK_DETECTED=false
for i in $(seq 1 $NODE_COUNT); do
    container="asteroidb-node-${i}"
    current_mem=$(get_container_memory_mb "$container")
    baseline="${BASELINE_MEM[$i]}"
    if [ -z "$baseline" ] || [ "$baseline" = "0" ]; then
        echo "  node-${i}: baseline=N/A, current=${current_mem} MiB (skipping)"
        continue
    fi
    # Calculate growth ratio using python3 for floating-point.
    growth=$(python3 -c "
b = float('${baseline}')
c = float('${current_mem}')
if b > 0:
    ratio = c / b
    print(f'{ratio:.2f}')
else:
    print('N/A')
" 2>/dev/null || echo "N/A")
    echo "  node-${i}: baseline=${baseline} MiB, current=${current_mem} MiB, growth=${growth}x"
    # Flag if memory grew more than 3x from baseline (potential leak).
    is_leak=$(python3 -c "
b = float('${baseline}')
c = float('${current_mem}')
if b > 0 and c / b > 3.0:
    print('true')
else:
    print('false')
" 2>/dev/null || echo "false")
    if [ "$is_leak" = "true" ]; then
        echo -e "  ${CLR_RED}[WARN] node-${i}: memory grew >3x — potential leak${CLR_RESET}"
        LEAK_DETECTED=true
    fi
done
echo ""

# =====================================================================
# Step 5: Convergence verification
# =====================================================================
log_step 5 "Convergence verification (write to node-1, check all nodes)"

CONV_KEY="scale-convergence-check-$$"
write_eventual_key "3001" "$CONV_KEY" "convergence-value"

echo "  Wrote convergence key. Waiting for propagation..."

CONV_TIMEOUT=60
CONV_INTERVAL=2
CONV_FAILURES=0

for port in "${NODE_PORTS[@]}"; do
    converged=false
    for attempt in $(seq 1 $((CONV_TIMEOUT / CONV_INTERVAL))); do
        resp=$(read_eventual_key "$port" "$CONV_KEY")
        if echo "$resp" | grep -q '"convergence-value"' 2>/dev/null; then
            converged=true
            break
        fi
        sleep "$CONV_INTERVAL"
    done
    if $converged; then
        echo -e "  ${CLR_GREEN}port ${port}: converged${CLR_RESET}"
    else
        echo -e "  ${CLR_RED}port ${port}: FAILED to converge within ${CONV_TIMEOUT}s${CLR_RESET}"
        CONV_FAILURES=$((CONV_FAILURES + 1))
    fi
done
echo ""

# =====================================================================
# Step 6: Node join/leave during sustained load
# =====================================================================
log_step 6 "Node join/leave test during sustained load"

# Start a short burst of writes in the background.
JOIN_LEAVE_DURATION=30
JOIN_LEAVE_END=$(( $(date +%s) + JOIN_LEAVE_DURATION ))
JOIN_LEAVE_KEY_PREFIX="join-leave-$$"
JL_COUNT_FILE=$(mktemp)
echo "0" > "$JL_COUNT_FILE"

jl_writer() {
    local end_ts="$1"
    local count=0
    while [ "$(date +%s)" -lt "$end_ts" ]; do
        local port_idx=$(( count % 3 ))  # Write to nodes 1-3 only (stable nodes).
        local port="${NODE_PORTS[$port_idx]}"
        write_counter_inc "$port" "${JOIN_LEAVE_KEY_PREFIX}"
        count=$((count + 1))
        # Small delay to avoid overwhelming.
        sleep 0.1
    done
    flock "$JL_COUNT_FILE" bash -c "echo \$(( \$(cat '$JL_COUNT_FILE') + $count )) > '$JL_COUNT_FILE'"
}

# Launch 3 background writers.
JL_PIDS=()
for w in $(seq 1 3); do
    jl_writer "$JOIN_LEAVE_END" &
    JL_PIDS+=($!)
done

echo "  Writers active. Testing node leave (stopping node-7)..."
sleep 5

# Remove node-7.
docker compose -f "$COMPOSE_FILE" stop node-7 2>/dev/null
echo "  node-7 stopped."

sleep 5

# Bring node-7 back.
echo "  Rejoining node-7..."
docker compose -f "$COMPOSE_FILE" start node-7 2>/dev/null
echo "  node-7 restarted."

# Wait for node-7 to be healthy again.
n7_wait=0
while [ "$n7_wait" -lt 30 ]; do
    if check_node_health "3007"; then
        echo -e "  ${CLR_GREEN}node-7 rejoined and healthy.${CLR_RESET}"
        break
    fi
    sleep 1
    n7_wait=$((n7_wait + 1))
done
if [ "$n7_wait" -ge 30 ]; then
    echo -e "  ${CLR_RED}node-7 did not rejoin within 30s.${CLR_RESET}"
fi

# Wait for background writers.
for pid in "${JL_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

JL_WRITES=$(cat "$JL_COUNT_FILE")
rm -f "$JL_COUNT_FILE"

echo "  Total counter increments during join/leave: ${JL_WRITES}"

# Verify the counter converged on all nodes.
echo "  Waiting 15s for convergence after join/leave..."
sleep 15

# Read the counter value from node-1 as reference.
REF_JSON=$(read_eventual_key "3001" "$JOIN_LEAVE_KEY_PREFIX")
REF_VAL=$(echo "$REF_JSON" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        print('null')
    elif isinstance(v, dict):
        print(v.get('value', 'null'))
    else:
        print(v)
except Exception:
    print('null')
" 2>/dev/null || echo "null")
echo "  Reference counter value (node-1): ${REF_VAL}"

JL_CONV_FAILURES=0
for port in "${NODE_PORTS[@]}"; do
    resp=$(read_eventual_key "$port" "$JOIN_LEAVE_KEY_PREFIX")
    val=$(echo "$resp" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        print('null')
    elif isinstance(v, dict):
        print(v.get('value', 'null'))
    else:
        print(v)
except Exception:
    print('null')
" 2>/dev/null || echo "null")
    if [ "$val" = "$REF_VAL" ]; then
        echo -e "  ${CLR_GREEN}port ${port}: counter=${val} (matches reference)${CLR_RESET}"
    else
        echo -e "  ${CLR_YELLOW}port ${port}: counter=${val} (reference=${REF_VAL})${CLR_RESET}"
        JL_CONV_FAILURES=$((JL_CONV_FAILURES + 1))
    fi
done
echo ""

# =====================================================================
# Step 7: Final metrics collection
# =====================================================================
log_step 7 "Final metrics and summary"

collect_metrics

# Count CSV data rows (exclude header).
METRIC_SAMPLES=$(( $(wc -l < "$METRICS_CSV") - 1 ))

TOTAL_DURATION=$(( $(date +%s) - TEST_START ))

TOTAL_CONV_FAILURES=$((CONV_FAILURES + JL_CONV_FAILURES))

# =====================================================================
# Output results
# =====================================================================
separator
echo -e "${CLR_BOLD}Large-Scale Test Results${CLR_RESET}"
sub_separator

cat <<SUMMARY
  Nodes:                ${NODE_COUNT}
  Duration:             ${TOTAL_DURATION}s
  Total writes:         ${WRITE_COUNT}
  Write errors:         ${WRITE_ERRORS}
  Throughput:           ~${THROUGHPUT} ops/sec
  Convergence failures: ${TOTAL_CONV_FAILURES}
  Memory leak detected: ${LEAK_DETECTED}
  Metric samples:       ${METRIC_SAMPLES}
  CSV output:           ${METRICS_CSV}
SUMMARY

# Write JSON results.
cat > "$RESULTS_JSON" <<EOF
{
  "test": "large-scale",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "config": {
    "node_count": ${NODE_COUNT},
    "duration_s": ${DURATION},
    "concurrency": ${CONCURRENCY},
    "sample_interval_s": ${SAMPLE_INTERVAL}
  },
  "results": {
    "total_duration_s": ${TOTAL_DURATION},
    "total_writes": ${WRITE_COUNT},
    "write_errors": ${WRITE_ERRORS},
    "throughput_ops_sec": ${THROUGHPUT},
    "convergence_failures": ${TOTAL_CONV_FAILURES},
    "memory_leak_detected": ${LEAK_DETECTED},
    "join_leave_writes": ${JL_WRITES},
    "metric_samples": ${METRIC_SAMPLES}
  }
}
EOF

echo "  JSON results: ${RESULTS_JSON}"
separator

# --- Exit status ---
if [ "$TOTAL_CONV_FAILURES" -gt 0 ]; then
    echo -e "${CLR_RED}[FAIL] Convergence failures detected.${CLR_RESET}"
    exit 1
fi

if [ "$LEAK_DETECTED" = "true" ]; then
    echo -e "${CLR_YELLOW}[WARN] Potential memory leak detected. Review ${METRICS_CSV}.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}[PASS] Large-scale test completed successfully.${CLR_RESET}"
exit 0
