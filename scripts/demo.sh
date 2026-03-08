#!/usr/bin/env bash
# AsteroidDB interactive demo script.
# Brings up a 3-node cluster, demonstrates eventual and certified writes,
# simulates a network partition, and verifies CRDT convergence after healing.
#
# Usage: scripts/demo.sh
#
# Requires: docker compose, curl, jq

set -euo pipefail

# --------------------------------------------------------------------------
# Colors and helpers
# --------------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

NODE1="http://localhost:3001"
NODE2="http://localhost:3002"
NODE3="http://localhost:3003"

step_num=0

step() {
    step_num=$((step_num + 1))
    echo ""
    echo -e "${BOLD}${BLUE}=== Step ${step_num}: $1 ===${NC}"
    echo ""
}

info() {
    echo -e "  ${CYAN}[info]${NC} $1"
}

ok() {
    echo -e "  ${GREEN}[ok]${NC} $1"
}

warn() {
    echo -e "  ${YELLOW}[warn]${NC} $1"
}

fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
}

run_cmd() {
    echo -e "  ${YELLOW}\$ $1${NC}"
    eval "$1" 2>&1 | sed 's/^/    /'
    echo ""
}

wait_for_node() {
    local url="$1"
    local name="$2"
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -sf --max-time 2 "${url}/api/eventual/__health" > /dev/null 2>&1; then
            return 0
        fi
        # A 200 with a null value body also means the server is up.
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 "${url}/api/eventual/__health" 2>/dev/null || echo "000")
        if [ "$code" = "200" ]; then
            return 0
        fi
        retries=$((retries - 1))
        sleep 1
    done
    fail "${name} did not become healthy in 30s"
    return 1
}

# --------------------------------------------------------------------------
# Cleanup on exit
# --------------------------------------------------------------------------

cleanup() {
    echo ""
    echo -e "${BOLD}${BLUE}=== Cleanup ===${NC}"
    info "Stopping cluster..."
    docker compose -f "$PROJECT_DIR/docker-compose.yml" down --timeout 5 > /dev/null 2>&1 || true
    ok "Cluster stopped."
}

trap cleanup EXIT

# --------------------------------------------------------------------------
# Demo flow
# --------------------------------------------------------------------------

echo -e "${BOLD}${GREEN}"
echo "  ___        _                 _     _ ____  ____  "
echo " / _ \      | |               (_)   | |  _ \|  _ \ "
echo "| |_| |_____| |_ ___ _ __ ___  _  __| | |_) | |_) |"
echo "|  _  |_____| __/ _ \ '__/ _ \| |/ _\` |  _ <|  _ < "
echo "| | | |     | ||  __/ | | (_) | | (_| | |_) | |_) |"
echo "|_| |_|      \__\___|_|  \___/|_|\__,_|____/|____/ "
echo ""
echo "  Interactive Demo"
echo -e "${NC}"

# -- Step 1: Start cluster ------------------------------------------------

step "Start 3-node cluster"

info "Building and starting containers..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --build 2>&1 | sed 's/^/    /'

info "Waiting for nodes to become healthy..."
wait_for_node "$NODE1" "node-1" && ok "node-1 is up (localhost:3001)"
wait_for_node "$NODE2" "node-2" && ok "node-2 is up (localhost:3002)"
wait_for_node "$NODE3" "node-3" && ok "node-3 is up (localhost:3003)"

# -- Step 2: Eventual write on node-1 -------------------------------------

step "Write to node-1 (eventual mode)"

info "Writing key 'sensor-1' with value '23.5' to node-1..."
run_cmd "curl -s -X POST ${NODE1}/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{\"type\":\"register_set\",\"key\":\"sensor-1\",\"value\":\"23.5\"}'"

info "Reading key 'sensor-1' from node-1..."
run_cmd "curl -s ${NODE1}/api/eventual/sensor-1 | jq ."

# -- Step 3: Read from node-2 (after sync) --------------------------------

step "Read from node-2 (verify replication)"

info "Waiting a few seconds for delta sync..."
sleep 5

info "Reading key 'sensor-1' from node-2..."
run_cmd "curl -s ${NODE2}/api/eventual/sensor-1 | jq ."

VALUE=$(curl -s "${NODE2}/api/eventual/sensor-1" 2>/dev/null | jq -r '.value.value // empty' 2>/dev/null || true)
if [ "$VALUE" = "23.5" ]; then
    ok "Replication confirmed: node-2 has the value."
else
    warn "Value not yet replicated (got: '${VALUE}'). Delta sync may need more time."
fi

# -- Step 4: CRDT counter demo --------------------------------------------

step "CRDT counter: concurrent increments"

info "Incrementing 'page-views' on node-1..."
run_cmd "curl -s -X POST ${NODE1}/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{\"type\":\"counter_inc\",\"key\":\"page-views\"}'"

info "Incrementing 'page-views' on node-2..."
run_cmd "curl -s -X POST ${NODE2}/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{\"type\":\"counter_inc\",\"key\":\"page-views\"}'"

info "Incrementing 'page-views' on node-3..."
run_cmd "curl -s -X POST ${NODE3}/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{\"type\":\"counter_inc\",\"key\":\"page-views\"}'"

info "Waiting for sync..."
sleep 5

info "Reading counter from node-1:"
run_cmd "curl -s ${NODE1}/api/eventual/page-views | jq ."

# -- Step 5: Simulate partition (pause node-3) -----------------------------

step "Simulate network partition (pause node-3)"

info "Pausing node-3 container..."
docker pause asteroidb-node-3 2>/dev/null || warn "Could not pause node-3"
ok "node-3 is now isolated."

# -- Step 6: Write during partition ----------------------------------------

step "Write during partition"

info "Writing 'sensor-1' = '99.9' on node-1 (node-3 is down)..."
run_cmd "curl -s -X POST ${NODE1}/api/eventual/write \
  -H 'Content-Type: application/json' \
  -d '{\"type\":\"register_set\",\"key\":\"sensor-1\",\"value\":\"99.9\"}'"

info "Writing 'sensor-1' = '0.1' on node-3 would fail (paused). Skipping."

info "Reading from node-1:"
run_cmd "curl -s ${NODE1}/api/eventual/sensor-1 | jq ."

info "Reading from node-3 (should be unreachable):"
curl -sf --max-time 2 "${NODE3}/api/eventual/sensor-1" 2>/dev/null \
    && warn "node-3 responded (unexpected)" \
    || ok "node-3 is unreachable as expected."

# -- Step 7: Heal partition ------------------------------------------------

step "Heal partition (unpause node-3)"

info "Unpausing node-3 container..."
docker unpause asteroidb-node-3 2>/dev/null || warn "Could not unpause node-3"
ok "node-3 is back online."

info "Waiting for anti-entropy sync to converge..."
sleep 8

# -- Step 8: Verify convergence -------------------------------------------

step "Verify convergence after partition heals"

info "Reading 'sensor-1' from all nodes:"

V1=$(curl -s "${NODE1}/api/eventual/sensor-1" 2>/dev/null | jq -r '.value.value // empty' 2>/dev/null || true)
V2=$(curl -s "${NODE2}/api/eventual/sensor-1" 2>/dev/null | jq -r '.value.value // empty' 2>/dev/null || true)
V3=$(curl -s "${NODE3}/api/eventual/sensor-1" 2>/dev/null | jq -r '.value.value // empty' 2>/dev/null || true)

echo -e "  node-1: ${GREEN}${V1}${NC}"
echo -e "  node-2: ${GREEN}${V2}${NC}"
echo -e "  node-3: ${GREEN}${V3}${NC}"

if [ "$V1" = "$V2" ] && [ "$V2" = "$V3" ] && [ -n "$V1" ]; then
    ok "All nodes converged to the same value: ${V1}"
else
    warn "Nodes have not fully converged yet. This is expected if sync needs more time."
    info "  node-1=${V1}  node-2=${V2}  node-3=${V3}"
fi

# -- Step 9: Certified write + read ---------------------------------------

step "Certified write and read"

info "Performing a certified write on node-1..."
run_cmd "curl -s -X POST ${NODE1}/api/certified/write \
  -H 'Content-Type: application/json' \
  -d '{\"key\":\"balance\",\"value\":{\"type\":\"register\",\"value\":\"1000\"},\"on_timeout\":\"pending\"}' | jq ."

info "Reading certified value from node-1..."
run_cmd "curl -s ${NODE1}/api/certified/balance | jq ."

info "Checking certification status..."
run_cmd "curl -s ${NODE1}/api/status/balance | jq ."

# -- Step 10: Metrics and SLO ---------------------------------------------

step "Check metrics and SLO"

info "Node-1 metrics:"
run_cmd "curl -s ${NODE1}/api/metrics | jq ."

info "SLO budget status:"
run_cmd "curl -s ${NODE1}/api/slo | jq ."

# -- Done -----------------------------------------------------------------

echo ""
echo -e "${BOLD}${GREEN}=== Demo complete ===${NC}"
echo ""
echo -e "  The cluster demonstrated:"
echo -e "    1. Eventual writes with CRDT replication"
echo -e "    2. Cross-node convergence via delta sync"
echo -e "    3. Concurrent CRDT counter increments"
echo -e "    4. Partition tolerance (writes continue during isolation)"
echo -e "    5. Automatic convergence after partition heals"
echo -e "    6. Certified writes with authority consensus"
echo -e "    7. Runtime metrics and SLO monitoring"
echo ""
echo -e "  ${CYAN}Cluster is still running. It will be stopped on exit.${NC}"
echo -e "  ${CYAN}Press Ctrl-C to stop, or explore manually:${NC}"
echo -e "    node-1: ${NODE1}"
echo -e "    node-2: ${NODE2}"
echo -e "    node-3: ${NODE3}"
echo ""

# Keep running so the user can explore; cleanup runs on EXIT.
read -r -p "  Press Enter to stop the cluster and exit..." || true
