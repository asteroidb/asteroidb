#!/usr/bin/env bash
# bench-multinode.sh — Multi-node throughput and latency benchmark.
#
# Spins up a 3-node AsteroidDB cluster via docker compose, fires concurrent
# writes, measures throughput (ops/sec) and latency percentiles (p50/p95/p99),
# and outputs results as JSON.
#
# Usage:
#   ./scripts/bench-multinode.sh [--ops N] [--concurrency C]
#
# Requirements: docker, docker compose, curl, jq, bc

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults.
TOTAL_OPS=500
CONCURRENCY=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ops) TOTAL_OPS="$2"; shift 2 ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

# Node endpoints (mapped ports from docker-compose.yml).
NODES=("http://localhost:3001" "http://localhost:3002" "http://localhost:3003")

# Generate a random internal token if not set.
export ASTEROIDB_INTERNAL_TOKEN="${ASTEROIDB_INTERNAL_TOKEN:-bench-token-$(date +%s)}"

cleanup() {
    echo "Tearing down cluster..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------
# 1. Start cluster
# ---------------------------------------------------------------
echo "=== Multi-Node Benchmark ==="
echo "Ops: $TOTAL_OPS  Concurrency: $CONCURRENCY"
echo ""
echo "Building and starting 3-node cluster..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --build --wait 2>&1 | tail -1

# Wait for all nodes to be healthy.
MAX_WAIT=60
for node_url in "${NODES[@]}"; do
    echo -n "Waiting for $node_url..."
    waited=0
    until curl -sf "$node_url/status" >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge $MAX_WAIT ]; then
            echo " TIMEOUT"
            echo "ERROR: Node $node_url did not become ready within ${MAX_WAIT}s"
            exit 1
        fi
    done
    echo " ready"
done

echo ""

# ---------------------------------------------------------------
# 2. Run concurrent writes and collect latencies
# ---------------------------------------------------------------
LATENCY_FILE=$(mktemp)

run_writes() {
    local worker_id=$1
    local ops_per_worker=$2
    local node_url=${NODES[$((worker_id % ${#NODES[@]}))]}

    for i in $(seq 1 "$ops_per_worker"); do
        local key="bench/worker-${worker_id}/key-${i}"
        local start_ns
        start_ns=$(date +%s%N)

        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
            -X POST "$node_url/api/eventual/write" \
            -H "Content-Type: application/json" \
            -d "{\"key\": \"$key\", \"value\": {\"Counter\": {\"p\": {\"bench-node\": 1}, \"n\": {}}}}" \
            2>/dev/null) || http_code="000"

        local end_ns
        end_ns=$(date +%s%N)
        local latency_us=$(( (end_ns - start_ns) / 1000 ))

        echo "$latency_us $http_code"
    done
}

OPS_PER_WORKER=$((TOTAL_OPS / CONCURRENCY))
echo "Launching $CONCURRENCY workers, $OPS_PER_WORKER ops each..."
echo ""

OVERALL_START=$(date +%s%N)

pids=()
for w in $(seq 0 $((CONCURRENCY - 1))); do
    run_writes "$w" "$OPS_PER_WORKER" >> "$LATENCY_FILE" &
    pids+=($!)
done

# Wait for all workers.
for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
done

OVERALL_END=$(date +%s%N)
OVERALL_DURATION_MS=$(( (OVERALL_END - OVERALL_START) / 1000000 ))

# ---------------------------------------------------------------
# 3. Compute statistics
# ---------------------------------------------------------------
TOTAL_REQUESTS=$(wc -l < "$LATENCY_FILE")
SUCCESS_COUNT=$(awk '$2 >= 200 && $2 < 300 {c++} END {print c+0}' "$LATENCY_FILE")
ERROR_COUNT=$((TOTAL_REQUESTS - SUCCESS_COUNT))

if [ "$OVERALL_DURATION_MS" -gt 0 ]; then
    THROUGHPUT=$(echo "scale=2; $TOTAL_REQUESTS * 1000 / $OVERALL_DURATION_MS" | bc)
else
    THROUGHPUT="0"
fi

# Sort latencies (microseconds) for percentile calculation.
SORTED_LATENCIES=$(awk '{print $1}' "$LATENCY_FILE" | sort -n)

percentile() {
    local pct=$1
    local count
    count=$(echo "$SORTED_LATENCIES" | wc -l)
    if [ "$count" -eq 0 ]; then
        echo "0"
        return
    fi
    local idx
    idx=$(echo "scale=0; ($count * $pct + 99) / 100" | bc)
    if [ "$idx" -lt 1 ]; then idx=1; fi
    echo "$SORTED_LATENCIES" | sed -n "${idx}p"
}

P50=$(percentile 50)
P95=$(percentile 95)
P99=$(percentile 99)

# Convert microseconds to milliseconds for display.
p50_ms=$(echo "scale=2; $P50 / 1000" | bc)
p95_ms=$(echo "scale=2; $P95 / 1000" | bc)
p99_ms=$(echo "scale=2; $P99 / 1000" | bc)

# ---------------------------------------------------------------
# 4. Output results
# ---------------------------------------------------------------
echo "=== Results ==="
echo "  Total requests: $TOTAL_REQUESTS"
echo "  Successful:     $SUCCESS_COUNT"
echo "  Errors:         $ERROR_COUNT"
echo "  Duration:       ${OVERALL_DURATION_MS}ms"
echo "  Throughput:     ${THROUGHPUT} ops/sec"
echo "  Latency p50:    ${p50_ms}ms"
echo "  Latency p95:    ${p95_ms}ms"
echo "  Latency p99:    ${p99_ms}ms"
echo ""

# JSON output.
RESULTS_JSON=$(cat <<EOF
{
  "benchmark": "multinode-throughput",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "config": {
    "total_ops": $TOTAL_OPS,
    "concurrency": $CONCURRENCY,
    "nodes": ${#NODES[@]}
  },
  "results": {
    "total_requests": $TOTAL_REQUESTS,
    "success_count": $SUCCESS_COUNT,
    "error_count": $ERROR_COUNT,
    "duration_ms": $OVERALL_DURATION_MS,
    "throughput_ops_sec": $THROUGHPUT,
    "latency_us": {
      "p50": $P50,
      "p95": $P95,
      "p99": $P99
    }
  }
}
EOF
)

RESULTS_FILE="$PROJECT_DIR/target/bench-multinode-results.json"
mkdir -p "$(dirname "$RESULTS_FILE")"
echo "$RESULTS_JSON" > "$RESULTS_FILE"
echo "JSON results written to: $RESULTS_FILE"

rm -f "$LATENCY_FILE"
