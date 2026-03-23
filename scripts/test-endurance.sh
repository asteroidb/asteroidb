#!/usr/bin/env bash
# test-endurance.sh — Long-running endurance test for AsteroidDB.
#
# Runs a 30-minute (configurable) sustained mixed workload including
# eventual writes, counter increments, and register operations across a
# 7-node cluster.  Periodically partitions and heals nodes to simulate
# real-world fault patterns.  Tracks memory, convergence, and throughput
# throughout the test.
#
# Usage:
#   ./scripts/test-endurance.sh [OPTIONS]
#
# Options:
#   --duration SECONDS         Total test duration (default: 1800 = 30 min)
#   --partition-interval SECS  Seconds between partition/heal cycles (default: 120)
#   --partition-duration SECS  How long each partition lasts (default: 15)
#   --concurrency N            Number of concurrent writers (default: 5)
#   --sample-interval SECS     Metric sampling interval (default: 30)
#   --help                     Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - curl, python3 available on the host
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NETEM_DIR="${SCRIPT_DIR}/netem"

# Source shared helpers.
if [ -f "${NETEM_DIR}/lib.sh" ]; then
    source "${NETEM_DIR}/lib.sh"
else
    CLR_GREEN="" CLR_RED="" CLR_YELLOW="" CLR_BOLD="" CLR_RESET=""
    separator() { echo "======================================================================"; }
    sub_separator() { echo "----------------------------------------------------------------------"; }
    log_step() { local n="$1"; shift; separator; echo "STEP ${n}: $*"; sub_separator; }
fi

# --- Defaults ---
DURATION=1800
PARTITION_INTERVAL=120
PARTITION_DURATION=15
CONCURRENCY=5
SAMPLE_INTERVAL=30

COMPOSE_FILE="$PROJECT_DIR/docker-compose.scale.yml"
NODE_COUNT=7
HEALTH_TIMEOUT=120

declare -a NODE_PORTS
for i in $(seq 1 $NODE_COUNT); do
    NODE_PORTS+=("300${i}")
done

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/test-endurance.sh [OPTIONS]

Options:
  --duration SECONDS         Total test duration (default: 1800 = 30 min)
  --partition-interval SECS  Seconds between partition/heal cycles (default: 120)
  --partition-duration SECS  How long each partition lasts (default: 15)
  --concurrency N            Number of concurrent writers (default: 5)
  --sample-interval SECS     Metric sampling interval (default: 30)
  --help                     Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)            DURATION="${2:?requires value}"; shift 2 ;;
        --partition-interval)  PARTITION_INTERVAL="${2:?requires value}"; shift 2 ;;
        --partition-duration)  PARTITION_DURATION="${2:?requires value}"; shift 2 ;;
        --concurrency)         CONCURRENCY="${2:?requires value}"; shift 2 ;;
        --sample-interval)     SAMPLE_INTERVAL="${2:?requires value}"; shift 2 ;;
        --help)                usage; exit 0 ;;
        *)                     echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# --- Output files ---
RESULTS_DIR="$PROJECT_DIR/target"
mkdir -p "$RESULTS_DIR"
METRICS_CSV="$RESULTS_DIR/endurance-metrics.csv"
CONVERGENCE_LOG="$RESULTS_DIR/endurance-convergence.csv"
PARTITION_LOG="$RESULTS_DIR/endurance-partitions.csv"
RESULTS_JSON="$RESULTS_DIR/endurance-results.json"

# --- Token ---
export ASTEROIDB_INTERNAL_TOKEN="${ASTEROIDB_INTERNAL_TOKEN:-endurance-$(date +%s)}"

# --- Cleanup ---
cleanup() {
    echo ""
    echo "[endurance] Stopping background processes..."
    jobs -p 2>/dev/null | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true
    # Remove any iptables rules that might be left.
    for i in $(seq 1 $NODE_COUNT); do
        docker exec "asteroidb-node-${i}" iptables -F OUTPUT 2>/dev/null || true
        docker exec "asteroidb-node-${i}" iptables -F INPUT 2>/dev/null || true
    done
    echo "[endurance] Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down --timeout 10 --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# --- Helpers ---

check_node_health() {
    local port="$1"
    curl -sf --max-time 3 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1
}

get_container_memory_mb() {
    local container="$1"
    docker stats --no-stream --format "{{.MemUsage}}" "$container" 2>/dev/null | awk -F'/' '{gsub(/[^0-9.]/, "", $1); print $1}'
}

get_container_cpu_pct() {
    local container="$1"
    docker stats --no-stream --format "{{.CPUPerc}}" "$container" 2>/dev/null | tr -d '%'
}

write_eventual() {
    local port="$1" key="$2" value="$3"
    curl -sf -X POST "http://localhost:${port}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"register_set\",\"key\":\"${key}\",\"value\":\"${value}\"}" \
        -o /dev/null --max-time 10 2>/dev/null || true
}

write_counter() {
    local port="$1" key="$2"
    curl -sf -X POST "http://localhost:${port}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"counter_inc\",\"key\":\"${key}\"}" \
        -o /dev/null --max-time 10 2>/dev/null || true
}

read_eventual() {
    local port="$1" key="$2"
    curl -sf --max-time 5 "http://localhost:${port}/api/eventual/${key}" 2>/dev/null || echo '{"value":null}'
}

collect_metrics() {
    local ts elapsed
    ts=$(date +%s)
    elapsed=$(( ts - TEST_START ))
    for i in $(seq 1 $NODE_COUNT); do
        local container="asteroidb-node-${i}"
        local mem cpu
        mem=$(get_container_memory_mb "$container")
        cpu=$(get_container_cpu_pct "$container")
        echo "${ts},${elapsed},node-${i},${mem:-0},${cpu:-0}" >> "$METRICS_CSV"
    done
}

# Partition a node by blocking traffic to/from all other nodes.
partition_node() {
    local target_node="$1"
    local target_container="asteroidb-node-${target_node}"
    for i in $(seq 1 $NODE_COUNT); do
        if [ "$i" -eq "$target_node" ]; then continue; fi
        local peer_ip
        peer_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "asteroidb-node-${i}" 2>/dev/null || echo "")
        if [ -n "$peer_ip" ]; then
            docker exec "$target_container" iptables -A OUTPUT -d "$peer_ip" -j DROP 2>/dev/null || true
            docker exec "$target_container" iptables -A INPUT -s "$peer_ip" -j DROP 2>/dev/null || true
        fi
    done
}

# Heal a previously partitioned node.
heal_node() {
    local target_node="$1"
    local target_container="asteroidb-node-${target_node}"
    docker exec "$target_container" iptables -F OUTPUT 2>/dev/null || true
    docker exec "$target_container" iptables -F INPUT 2>/dev/null || true
}

# Check convergence of a specific key across all nodes.
check_convergence_key() {
    local key="$1"
    local ref_port="3001"
    local ref_val
    ref_val=$(read_eventual "$ref_port" "$key")
    local failures=0
    for port in "${NODE_PORTS[@]}"; do
        local val
        val=$(read_eventual "$port" "$key")
        if [ "$val" != "$ref_val" ]; then
            failures=$((failures + 1))
        fi
    done
    echo "$failures"
}

# =====================================================================
# Start cluster
# =====================================================================
separator
echo -e "${CLR_BOLD}AsteroidDB Endurance Test${CLR_RESET}"
separator
echo ""
echo "  Duration:             ${DURATION}s ($(( DURATION / 60 ))m)"
echo "  Partition interval:   ${PARTITION_INTERVAL}s"
echo "  Partition duration:   ${PARTITION_DURATION}s"
echo "  Concurrency:          ${CONCURRENCY} writers"
echo "  Sample interval:      ${SAMPLE_INTERVAL}s"
echo "  Nodes:                ${NODE_COUNT}"
echo ""

log_step 1 "Starting ${NODE_COUNT}-node cluster"
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5

echo "[endurance] Waiting for cluster health..."
for port in "${NODE_PORTS[@]}"; do
    elapsed=0
    while [ "$elapsed" -lt "$HEALTH_TIMEOUT" ]; do
        if check_node_health "$port"; then
            echo "  port ${port}: UP"
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    if [ "$elapsed" -ge "$HEALTH_TIMEOUT" ]; then
        echo -e "  ${CLR_RED}port ${port}: TIMEOUT${CLR_RESET}" >&2
        exit 1
    fi
done
echo -e "${CLR_GREEN}All ${NODE_COUNT} nodes healthy.${CLR_RESET}"
echo ""

# =====================================================================
# Initialize tracking files
# =====================================================================
TEST_START=$(date +%s)
TEST_END=$(( TEST_START + DURATION ))

echo "timestamp_s,elapsed_s,node,memory_mb,cpu_pct" > "$METRICS_CSV"
echo "timestamp_s,elapsed_s,event,target_node,detail" > "$PARTITION_LOG"
echo "timestamp_s,elapsed_s,check_key,failures" > "$CONVERGENCE_LOG"

# Baseline metrics.
collect_metrics

# =====================================================================
# Background: mixed workload writers
# =====================================================================
log_step 2 "Starting mixed workload"

WRITE_TOTAL_FILE=$(mktemp)
WRITE_ERROR_FILE=$(mktemp)
echo "0" > "$WRITE_TOTAL_FILE"
echo "0" > "$WRITE_ERROR_FILE"

mixed_writer() {
    local worker_id="$1"
    local end_ts="$2"
    local count=0
    local errors=0
    while [ "$(date +%s)" -lt "$end_ts" ]; do
        local port_idx=$(( (worker_id + count) % NODE_COUNT ))
        local port="${NODE_PORTS[$port_idx]}"
        local op_type=$(( count % 3 ))  # 0=register, 1=counter, 2=register (2:1 ratio)
        local http_code

        if [ "$op_type" -eq 1 ]; then
            # Counter increment.
            http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
                -X POST "http://localhost:${port}/api/eventual/write" \
                -H "Content-Type: application/json" \
                -d "{\"type\":\"counter_inc\",\"key\":\"endurance/ctr/w${worker_id}\"}" \
                --max-time 10 2>/dev/null) || http_code="000"
        else
            # Register set.
            http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
                -X POST "http://localhost:${port}/api/eventual/write" \
                -H "Content-Type: application/json" \
                -d "{\"type\":\"register_set\",\"key\":\"endurance/reg/w${worker_id}/k${count}\",\"value\":\"v${count}\"}" \
                --max-time 10 2>/dev/null) || http_code="000"
        fi

        if [[ "$http_code" =~ ^2 ]]; then
            count=$((count + 1))
        else
            errors=$((errors + 1))
            count=$((count + 1))
        fi

        # Small throttle to avoid saturation.
        sleep 0.05
    done
    flock "$WRITE_TOTAL_FILE" bash -c "echo \$(( \$(cat '$WRITE_TOTAL_FILE') + $count )) > '$WRITE_TOTAL_FILE'"
    flock "$WRITE_ERROR_FILE" bash -c "echo \$(( \$(cat '$WRITE_ERROR_FILE') + $errors )) > '$WRITE_ERROR_FILE'"
}

# Launch writers.
WRITER_PIDS=()
for w in $(seq 0 $((CONCURRENCY - 1))); do
    mixed_writer "$w" "$TEST_END" &
    WRITER_PIDS+=($!)
done
echo "  ${CONCURRENCY} mixed writers launched."
echo ""

# =====================================================================
# Background: periodic metric collector
# =====================================================================
metric_collector() {
    local end_ts="$1"
    while [ "$(date +%s)" -lt "$end_ts" ]; do
        sleep "$SAMPLE_INTERVAL"
        collect_metrics
    done
}
metric_collector "$TEST_END" &
METRIC_PID=$!

# =====================================================================
# Main loop: partition/heal cycles + convergence checks
# =====================================================================
log_step 3 "Partition/heal cycles (every ${PARTITION_INTERVAL}s, hold ${PARTITION_DURATION}s)"

PARTITION_COUNT=0
CONVERGENCE_FAILURES_TOTAL=0
NEXT_PARTITION=$(( $(date +%s) + PARTITION_INTERVAL ))

while [ "$(date +%s)" -lt "$TEST_END" ]; do
    NOW=$(date +%s)
    ELAPSED=$(( NOW - TEST_START ))

    if [ "$NOW" -ge "$NEXT_PARTITION" ] && [ $((NOW + PARTITION_DURATION + 30)) -lt "$TEST_END" ]; then
        PARTITION_COUNT=$((PARTITION_COUNT + 1))

        # Pick a non-authority node to partition (nodes 4-7) to avoid breaking quorum.
        TARGET_NODE=$(( (PARTITION_COUNT % 4) + 4 ))

        echo "  [${ELAPSED}s] Partition #${PARTITION_COUNT}: isolating node-${TARGET_NODE} for ${PARTITION_DURATION}s"
        echo "${NOW},${ELAPSED},partition_start,${TARGET_NODE}," >> "$PARTITION_LOG"

        partition_node "$TARGET_NODE"
        sleep "$PARTITION_DURATION"

        HEAL_TS=$(date +%s)
        HEAL_ELAPSED=$(( HEAL_TS - TEST_START ))
        echo "  [${HEAL_ELAPSED}s] Healing node-${TARGET_NODE}"
        echo "${HEAL_TS},${HEAL_ELAPSED},partition_heal,${TARGET_NODE}," >> "$PARTITION_LOG"

        heal_node "$TARGET_NODE"

        # Wait a bit for convergence after heal.
        sleep 10

        NEXT_PARTITION=$(( $(date +%s) + PARTITION_INTERVAL ))
    fi

    # Periodic convergence check.
    CONV_KEY="endurance/conv-check-${ELAPSED}"
    write_eventual "3001" "$CONV_KEY" "check-${ELAPSED}"
    sleep 5
    CONV_FAILURES=$(check_convergence_key "$CONV_KEY")
    CONV_TS=$(date +%s)
    CONV_ELAPSED=$(( CONV_TS - TEST_START ))
    echo "${CONV_TS},${CONV_ELAPSED},${CONV_KEY},${CONV_FAILURES}" >> "$CONVERGENCE_LOG"

    if [ "$CONV_FAILURES" -gt 0 ]; then
        echo -e "  [${CONV_ELAPSED}s] ${CLR_YELLOW}Convergence check: ${CONV_FAILURES} node(s) not converged${CLR_RESET}"
        CONVERGENCE_FAILURES_TOTAL=$((CONVERGENCE_FAILURES_TOTAL + CONV_FAILURES))
    else
        echo "  [${CONV_ELAPSED}s] Convergence check: OK"
    fi

    # Sleep until next check cycle or test end.
    NEXT_CHECK=$(( $(date +%s) + 30 ))
    if [ "$NEXT_CHECK" -gt "$TEST_END" ]; then
        REMAINING=$(( TEST_END - $(date +%s) ))
        if [ "$REMAINING" -gt 0 ]; then
            sleep "$REMAINING"
        fi
    else
        sleep 30
    fi
done

echo ""
echo "  Partition/heal cycles completed: ${PARTITION_COUNT}"
echo ""

# =====================================================================
# Wait for writers and collector to finish
# =====================================================================
log_step 4 "Waiting for workload completion"

for pid in "${WRITER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
wait "$METRIC_PID" 2>/dev/null || true

TOTAL_WRITES=$(cat "$WRITE_TOTAL_FILE")
TOTAL_ERRORS=$(cat "$WRITE_ERROR_FILE")
rm -f "$WRITE_TOTAL_FILE" "$WRITE_ERROR_FILE"

echo "  Total operations: ${TOTAL_WRITES}"
echo "  Total errors:     ${TOTAL_ERRORS}"
echo ""

# =====================================================================
# Final convergence check
# =====================================================================
log_step 5 "Final convergence verification"

FINAL_KEY="endurance/final-check-$$"
write_eventual "3001" "$FINAL_KEY" "final-value"
echo "  Waiting 20s for final propagation..."
sleep 20

FINAL_FAILURES=0
for port in "${NODE_PORTS[@]}"; do
    resp=$(read_eventual "$port" "$FINAL_KEY")
    if echo "$resp" | grep -q '"final-value"' 2>/dev/null; then
        echo -e "  ${CLR_GREEN}port ${port}: converged${CLR_RESET}"
    else
        echo -e "  ${CLR_RED}port ${port}: NOT converged${CLR_RESET}"
        FINAL_FAILURES=$((FINAL_FAILURES + 1))
    fi
done
echo ""

# =====================================================================
# Final metrics + memory analysis
# =====================================================================
log_step 6 "Memory analysis and results"

collect_metrics

# Analyze memory trend: compare first and last samples per node.
LEAK_DETECTED=false
for i in $(seq 1 $NODE_COUNT); do
    first_mem=$(grep ",node-${i}," "$METRICS_CSV" | head -1 | cut -d',' -f4)
    last_mem=$(grep ",node-${i}," "$METRICS_CSV" | tail -1 | cut -d',' -f4)
    growth=$(python3 -c "
f = float('${first_mem:-0}')
l = float('${last_mem:-0}')
if f > 0:
    print(f'{l/f:.2f}')
else:
    print('N/A')
" 2>/dev/null || echo "N/A")
    echo "  node-${i}: first=${first_mem:-0} MiB, last=${last_mem:-0} MiB, growth=${growth}x"
    is_leak=$(python3 -c "
f = float('${first_mem:-0}')
l = float('${last_mem:-0}')
print('true' if f > 0 and l / f > 3.0 else 'false')
" 2>/dev/null || echo "false")
    if [ "$is_leak" = "true" ]; then
        echo -e "  ${CLR_RED}[WARN] node-${i}: memory grew >3x — potential leak${CLR_RESET}"
        LEAK_DETECTED=true
    fi
done
echo ""

TOTAL_DURATION=$(( $(date +%s) - TEST_START ))
METRIC_SAMPLES=$(( $(wc -l < "$METRICS_CSV") - 1 ))
PARTITION_EVENTS=$(( $(wc -l < "$PARTITION_LOG") - 1 ))

if [ "$TOTAL_DURATION" -gt 0 ]; then
    THROUGHPUT=$(( TOTAL_WRITES / TOTAL_DURATION ))
else
    THROUGHPUT=0
fi

# =====================================================================
# Summary
# =====================================================================
separator
echo -e "${CLR_BOLD}Endurance Test Results${CLR_RESET}"
sub_separator

cat <<SUMMARY
  Nodes:                  ${NODE_COUNT}
  Duration:               ${TOTAL_DURATION}s ($(( TOTAL_DURATION / 60 ))m)
  Total operations:       ${TOTAL_WRITES}
  Total errors:           ${TOTAL_ERRORS}
  Throughput:             ~${THROUGHPUT} ops/sec
  Partition cycles:       ${PARTITION_COUNT}
  Convergence failures:   ${CONVERGENCE_FAILURES_TOTAL} (during test)
  Final convergence fail: ${FINAL_FAILURES}
  Memory leak detected:   ${LEAK_DETECTED}
  Metric samples:         ${METRIC_SAMPLES}
  Output files:
    Metrics CSV:          ${METRICS_CSV}
    Convergence log:      ${CONVERGENCE_LOG}
    Partition log:        ${PARTITION_LOG}
SUMMARY

# Write JSON results.
cat > "$RESULTS_JSON" <<EOF
{
  "test": "endurance",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "config": {
    "node_count": ${NODE_COUNT},
    "duration_s": ${DURATION},
    "partition_interval_s": ${PARTITION_INTERVAL},
    "partition_duration_s": ${PARTITION_DURATION},
    "concurrency": ${CONCURRENCY},
    "sample_interval_s": ${SAMPLE_INTERVAL}
  },
  "results": {
    "total_duration_s": ${TOTAL_DURATION},
    "total_writes": ${TOTAL_WRITES},
    "total_errors": ${TOTAL_ERRORS},
    "throughput_ops_sec": ${THROUGHPUT},
    "partition_cycles": ${PARTITION_COUNT},
    "convergence_failures_during_test": ${CONVERGENCE_FAILURES_TOTAL},
    "final_convergence_failures": ${FINAL_FAILURES},
    "memory_leak_detected": ${LEAK_DETECTED},
    "metric_samples": ${METRIC_SAMPLES}
  }
}
EOF

echo ""
echo "  JSON results: ${RESULTS_JSON}"
separator

# --- Exit status ---
if [ "$FINAL_FAILURES" -gt 0 ]; then
    echo -e "${CLR_RED}[FAIL] Final convergence failures detected.${CLR_RESET}"
    exit 1
fi

if [ "$LEAK_DETECTED" = "true" ]; then
    echo -e "${CLR_YELLOW}[WARN] Potential memory leak detected.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}[PASS] Endurance test completed successfully.${CLR_RESET}"
exit 0
