#!/usr/bin/env bash
# Shared library for netem scenario scripts.
#
# Source this file at the top of scenario scripts:
#   SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#   source "${SCRIPT_DIR}/lib.sh"
#
# Provides:
#   - Color constants (CLR_GREEN, CLR_RED, CLR_YELLOW, CLR_CYAN, CLR_RESET)
#   - separator / sub_separator - visual output formatting
#   - log_step <n> <msg>        - numbered step header
#   - check_node <url> <name>   - verify a node is responding
#   - read_counter <url> <key>  - read a counter value via eventual API
#   - extract_value <json>      - pull counter value from API JSON response
#   - wait_for_convergence <expected> <url> <name> <retries> <interval>
#                               - poll a node until its counter matches expected

# --- Color constants ---
if [ -t 1 ]; then
    CLR_GREEN="\033[0;32m"
    CLR_RED="\033[0;31m"
    CLR_YELLOW="\033[0;33m"
    CLR_CYAN="\033[0;36m"
    CLR_BOLD="\033[1m"
    CLR_RESET="\033[0m"
else
    CLR_GREEN=""
    CLR_RED=""
    CLR_YELLOW=""
    CLR_CYAN=""
    CLR_BOLD=""
    CLR_RESET=""
fi

# --- Output formatting ---

separator() {
    echo "======================================================================"
}

sub_separator() {
    echo "----------------------------------------------------------------------"
}

# log_step <step_number> <message>
log_step() {
    local step_num="$1"
    shift
    separator
    echo -e "${CLR_BOLD}STEP ${step_num}: $*${CLR_RESET}"
    sub_separator
}

# --- Node interaction ---

# check_node <url> <name>
# Returns 0 if the node responds with HTTP 200, 1 otherwise.
check_node() {
    local url="$1"
    local name="$2"
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "${url}/api/eventual/__health_check" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo -e "  ${CLR_GREEN}${name}: UP${CLR_RESET}"
        return 0
    else
        echo -e "  ${CLR_RED}${name}: DOWN (HTTP ${status})${CLR_RESET}"
        return 1
    fi
}

# read_counter <url> <key>
# Prints the raw JSON response for the given key.
read_counter() {
    local url="$1"
    local key="$2"
    curl -sf --max-time 5 "${url}/api/eventual/${key}" 2>/dev/null || echo '{"value":null}'
}

# extract_value <json>
# Extracts the numeric counter value from an API JSON response.
# Handles: {"key":"...","value":{"type":"counter","value":N}}
extract_value() {
    local json="$1"
    echo "$json" | python3 -c "
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
" 2>/dev/null || echo "null"
}

# --- Convergence helpers ---

# wait_for_convergence <expected> <url> <name> <retries> <interval>
# Polls a node until its counter value matches <expected>.
# Returns 0 if converged, 1 if timed out.
wait_for_convergence() {
    local expected="$1"
    local url="$2"
    local name="$3"
    local retries="${4:-10}"
    local interval="${5:-2}"
    local key="${6:-netem-test-key}"

    for attempt in $(seq 1 "$retries"); do
        sleep "$interval"
        local json
        json=$(read_counter "$url" "$key")
        local val
        val=$(extract_value "$json")
        echo "  Attempt ${attempt}/${retries}: ${name} counter = ${val}"

        if [ "$val" = "$expected" ]; then
            echo -e "  ${CLR_GREEN}[OK] ${name} converged to ${expected}.${CLR_RESET}"
            return 0
        fi
    done

    echo -e "  ${CLR_YELLOW}[WARN] ${name} did not converge within the retry window.${CLR_RESET}"
    echo "  Current value: ${val}, expected: ${expected}"
    return 1
}

# --- Cluster validation ---

# check_cluster <node1_url> <node2_url> <node3_url>
# Returns 0 if all 3 nodes are up, 1 otherwise.
check_cluster() {
    local node1_url="$1"
    local node2_url="$2"
    local node3_url="$3"
    local all_up=true

    if ! check_node "$node1_url" "node-1"; then all_up=false; fi
    if ! check_node "$node2_url" "node-2"; then all_up=false; fi
    if ! check_node "$node3_url" "node-3"; then all_up=false; fi

    if ! $all_up; then
        echo ""
        echo -e "${CLR_RED}[ERROR] Not all nodes are up. Start the cluster first:${CLR_RESET}"
        echo "  ./scripts/cluster-up.sh"
        return 1
    fi

    echo ""
    echo -e "${CLR_GREEN}All nodes healthy.${CLR_RESET}"
    return 0
}

# --- Timing helpers ---

# now_epoch_ms - prints current time in milliseconds since epoch
now_epoch_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

# elapsed_ms <start_ms>
# Prints elapsed milliseconds since <start_ms>.
elapsed_ms() {
    local start="$1"
    local now
    now=$(now_epoch_ms)
    echo $(( now - start ))
}
