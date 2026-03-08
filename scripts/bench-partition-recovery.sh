#!/usr/bin/env bash
# Benchmark: partition recovery time measurement for AsteroidDB.
#
# Measures how long it takes for a partitioned node to fully converge
# after the partition heals.  Outputs structured JSON with timing data,
# suitable for inclusion in a research submission.
#
# Usage: ./scripts/bench-partition-recovery.sh [OPTIONS]
#
# Options:
#   --baseline-keys N      Number of keys to write in baseline phase (default 100)
#   --partition-keys N     Number of keys to write during partition (default 50)
#   --partition-secs N     Seconds the partition is held (default 10)
#   --convergence-timeout  Max seconds to wait for convergence (default 60)
#   --help                 Show this help message
#
# Prerequisites:
#   - Docker and docker compose available
#   - jq and curl installed on the host
#   - python3 available (for millisecond timestamps)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../docker-compose.yml"
NETEM_DIR="${SCRIPT_DIR}/netem"

source "${NETEM_DIR}/lib.sh"

# --- Defaults ---
BASELINE_KEYS=100
PARTITION_KEYS=50
PARTITION_SECS=10
CONVERGENCE_TIMEOUT=60

# --- Argument parsing ---
usage() {
    cat <<'USAGE'
Usage: ./scripts/bench-partition-recovery.sh [OPTIONS]

Options:
  --baseline-keys N      Number of keys to write in baseline phase (default 100)
  --partition-keys N     Number of keys to write during partition (default 50)
  --partition-secs N     Seconds the partition is held (default 10)
  --convergence-timeout  Max seconds to wait for convergence (default 60)
  --help                 Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline-keys)
            BASELINE_KEYS="${2:?--baseline-keys requires a value}"
            shift 2
            ;;
        --partition-keys)
            PARTITION_KEYS="${2:?--partition-keys requires a value}"
            shift 2
            ;;
        --partition-secs)
            PARTITION_SECS="${2:?--partition-secs requires a value}"
            shift 2
            ;;
        --convergence-timeout)
            CONVERGENCE_TIMEOUT="${2:?--convergence-timeout requires a value}"
            shift 2
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

NODE1_URL="http://localhost:3001"
NODE2_URL="http://localhost:3002"
NODE3_URL="http://localhost:3003"
NODE3_CONTAINER="asteroidb-node-3"

TOTAL_KEYS=$(( BASELINE_KEYS + PARTITION_KEYS ))
RUN_ID="bench-pr-$$"

# --- Cleanup trap ---
cleanup() {
    echo ""
    echo "[bench] Cleaning up..."
    # Remove iptables / netem rules from node-3
    docker exec "$NODE3_CONTAINER" iptables -F OUTPUT 2>/dev/null || true
    docker exec "$NODE3_CONTAINER" iptables -F INPUT 2>/dev/null || true
    "${NETEM_DIR}/remove-netem.sh" "$NODE3_CONTAINER" 2>/dev/null || true
    # Tear down the cluster
    docker compose -f "$COMPOSE_FILE" down --timeout 5 2>/dev/null || true
}
trap cleanup EXIT

# --- Helper: write a register key to a node ---
write_key() {
    local url="$1"
    local key="$2"
    local value="$3"
    curl -sf -X POST "${url}/api/eventual/write" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"register_set\",\"key\":\"${key}\",\"value\":\"${value}\"}" > /dev/null
}

# --- Helper: check if a key exists with expected value ---
check_key() {
    local url="$1"
    local key="$2"
    local expected_value="$3"
    local json
    json=$(curl -sf --max-time 5 "${url}/api/eventual/${key}" 2>/dev/null || echo '{"value":null}')
    local actual
    actual=$(echo "$json" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('value')
    if v is None:
        print('')
    elif isinstance(v, dict):
        print(v.get('value', ''))
    else:
        print(v)
except Exception:
    print('')
" 2>/dev/null || echo "")
    [ "$actual" = "$expected_value" ]
}

# --- Helper: count how many of the expected keys are present ---
count_present_keys() {
    local url="$1"
    local prefix="$2"
    local total="$3"
    local count=0
    for i in $(seq 1 "$total"); do
        local key="${prefix}-${i}"
        if check_key "$url" "$key" "v-${i}"; then
            count=$(( count + 1 ))
        fi
    done
    echo "$count"
}

# --- Start cluster ---
separator
echo -e "${CLR_BOLD}AsteroidDB Partition Recovery Benchmark${CLR_RESET}"
separator
echo ""
echo "  Baseline keys:       ${BASELINE_KEYS}"
echo "  Partition keys:      ${PARTITION_KEYS}"
echo "  Partition duration:  ${PARTITION_SECS}s"
echo "  Convergence timeout: ${CONVERGENCE_TIMEOUT}s"
echo ""

echo "[bench] Starting cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build --quiet-pull 2>&1 | tail -5

echo "[bench] Waiting for cluster health..."
for port in 3001 3002 3003; do
    for attempt in $(seq 1 30); do
        if curl -sf --max-time 2 "http://localhost:${port}/api/eventual/__health_check" > /dev/null 2>&1; then
            echo "  Node on port ${port} is ready"
            break
        fi
        if [ "$attempt" -eq 30 ]; then
            echo "[bench] ERROR: Node on port ${port} did not become ready" >&2
            exit 1
        fi
        sleep 1
    done
done
echo "[bench] Cluster is ready."
echo ""

# ====================================================================
# Phase 1: Baseline -- write keys to node-1, verify replication
# ====================================================================
log_step 1 "Baseline: write ${BASELINE_KEYS} keys to node-1"
for i in $(seq 1 "$BASELINE_KEYS"); do
    write_key "$NODE1_URL" "${RUN_ID}-${i}" "v-${i}"
done
echo "  Wrote ${BASELINE_KEYS} keys to node-1."

echo "  Waiting for replication to node-2 and node-3..."
REPLICATION_RETRIES=30
for attempt in $(seq 1 "$REPLICATION_RETRIES"); do
    sleep 2
    n2_count=$(count_present_keys "$NODE2_URL" "$RUN_ID" "$BASELINE_KEYS")
    n3_count=$(count_present_keys "$NODE3_URL" "$RUN_ID" "$BASELINE_KEYS")
    echo "  Attempt ${attempt}/${REPLICATION_RETRIES}: node-2=${n2_count}/${BASELINE_KEYS}, node-3=${n3_count}/${BASELINE_KEYS}"
    if [ "$n2_count" -eq "$BASELINE_KEYS" ] && [ "$n3_count" -eq "$BASELINE_KEYS" ]; then
        echo -e "  ${CLR_GREEN}[OK] Baseline replication complete.${CLR_RESET}"
        break
    fi
    if [ "$attempt" -eq "$REPLICATION_RETRIES" ]; then
        echo -e "  ${CLR_RED}[ERROR] Baseline replication did not complete in time.${CLR_RESET}" >&2
        exit 1
    fi
done
echo ""

# ====================================================================
# Phase 2: Partition -- isolate node-3 from node-1 and node-2
# ====================================================================
log_step 2 "Partition: isolate node-3 using iptables"

# Get IPs of node-1 and node-2 as seen inside the Docker network.
NODE1_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' asteroidb-node-1)
NODE2_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' asteroidb-node-2)

echo "  node-1 IP: ${NODE1_IP}"
echo "  node-2 IP: ${NODE2_IP}"

# Block bidirectional traffic between node-3 and (node-1, node-2).
# On node-3: drop outgoing to node-1 and node-2, and incoming from them.
docker exec "$NODE3_CONTAINER" iptables -A OUTPUT -d "$NODE1_IP" -j DROP
docker exec "$NODE3_CONTAINER" iptables -A OUTPUT -d "$NODE2_IP" -j DROP
docker exec "$NODE3_CONTAINER" iptables -A INPUT -s "$NODE1_IP" -j DROP
docker exec "$NODE3_CONTAINER" iptables -A INPUT -s "$NODE2_IP" -j DROP

PARTITION_START_MS=$(now_epoch_ms)
echo "  Partition active at $(date '+%H:%M:%S'). Holding for ${PARTITION_SECS}s..."
echo ""

# ====================================================================
# Phase 3: Write during partition -- write keys to node-1
# ====================================================================
log_step 3 "Write ${PARTITION_KEYS} keys to node-1 during partition"

PARTITION_WRITE_START=$(( BASELINE_KEYS + 1 ))
PARTITION_WRITE_END=$(( BASELINE_KEYS + PARTITION_KEYS ))

for i in $(seq "$PARTITION_WRITE_START" "$PARTITION_WRITE_END"); do
    write_key "$NODE1_URL" "${RUN_ID}-${i}" "v-${i}"
done
echo "  Wrote ${PARTITION_KEYS} keys to node-1 during partition."

# Wait for partition duration to elapse (minus time already spent writing).
ELAPSED_PARTITION=$(( $(now_epoch_ms) - PARTITION_START_MS ))
REMAINING_MS=$(( PARTITION_SECS * 1000 - ELAPSED_PARTITION ))
if [ "$REMAINING_MS" -gt 0 ]; then
    REMAINING_SECS=$(python3 -c "print(${REMAINING_MS}/1000)")
    echo "  Holding partition for ${REMAINING_SECS}s more..."
    sleep "$REMAINING_SECS"
fi

PARTITION_END_MS=$(now_epoch_ms)
PARTITION_DURATION_MS=$(( PARTITION_END_MS - PARTITION_START_MS ))
echo ""

# ====================================================================
# Phase 4: Heal -- remove iptables rules, start convergence timer
# ====================================================================
log_step 4 "Heal: remove iptables rules on node-3"
docker exec "$NODE3_CONTAINER" iptables -F OUTPUT
docker exec "$NODE3_CONTAINER" iptables -F INPUT
HEAL_MS=$(now_epoch_ms)
echo "  Partition healed at $(date '+%H:%M:%S')."
echo "  Starting convergence timer..."
echo ""

# ====================================================================
# Phase 5: Measure convergence -- poll node-3 for all keys
# ====================================================================
log_step 5 "Measure convergence: poll node-3 for ${TOTAL_KEYS} keys"

CONVERGED=false
CONVERGENCE_TIME_MS=0
for attempt in $(seq 1 "$CONVERGENCE_TIMEOUT"); do
    sleep 1
    present=$(count_present_keys "$NODE3_URL" "$RUN_ID" "$TOTAL_KEYS")
    elapsed=$(elapsed_ms "$HEAL_MS")
    echo "  [${elapsed}ms] node-3 has ${present}/${TOTAL_KEYS} keys"

    if [ "$present" -eq "$TOTAL_KEYS" ]; then
        CONVERGENCE_TIME_MS="$elapsed"
        CONVERGED=true
        echo -e "  ${CLR_GREEN}[OK] node-3 converged in ${CONVERGENCE_TIME_MS}ms.${CLR_RESET}"
        break
    fi
done

if [ "$CONVERGED" = "false" ]; then
    CONVERGENCE_TIME_MS=$(elapsed_ms "$HEAL_MS")
    echo -e "  ${CLR_RED}[TIMEOUT] node-3 did not converge within ${CONVERGENCE_TIMEOUT}s.${CLR_RESET}"
fi

# Final verification: count verified keys on node-3.
VERIFIED=$(count_present_keys "$NODE3_URL" "$RUN_ID" "$TOTAL_KEYS")
DATA_LOSS=$(( TOTAL_KEYS - VERIFIED ))

echo ""

# ====================================================================
# Output results as JSON
# ====================================================================
separator
echo -e "${CLR_BOLD}Results${CLR_RESET}"
sub_separator

RESULT_JSON=$(cat <<EOF
{
  "partition_duration_ms": ${PARTITION_DURATION_MS},
  "convergence_time_ms": ${CONVERGENCE_TIME_MS},
  "keys_written": ${TOTAL_KEYS},
  "keys_verified": ${VERIFIED},
  "data_loss": ${DATA_LOSS},
  "baseline_keys": ${BASELINE_KEYS},
  "partition_keys": ${PARTITION_KEYS},
  "converged": ${CONVERGED}
}
EOF
)

echo "$RESULT_JSON"
echo ""

# Write to file for CI/CD consumption.
RESULT_FILE="${SCRIPT_DIR}/../target/bench-partition-recovery.json"
mkdir -p "$(dirname "$RESULT_FILE")"
echo "$RESULT_JSON" > "$RESULT_FILE"
echo "  Results written to: ${RESULT_FILE}"

separator

if [ "$DATA_LOSS" -gt 0 ]; then
    echo -e "${CLR_RED}[FAIL] Data loss detected: ${DATA_LOSS} keys missing.${CLR_RESET}"
    exit 1
fi

if [ "$CONVERGED" = "false" ]; then
    echo -e "${CLR_YELLOW}[WARN] Convergence timed out but no data loss detected.${CLR_RESET}"
    exit 1
fi

echo -e "${CLR_GREEN}[PASS] Partition recovery benchmark complete. Zero data loss.${CLR_RESET}"
exit 0
