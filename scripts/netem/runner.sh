#!/usr/bin/env bash
# Netem scenario runner for AsteroidDB.
#
# Reads scenario definitions from scenarios.json, executes them with
# timeout enforcement, and produces structured JSON results.
#
# Usage: ./scripts/netem/runner.sh [OPTIONS]
#
# Options:
#   --scenario <name>   Run a specific scenario by name
#   --all               Run all scenarios
#   --list              List available scenarios
#   --json-output       Output results as JSON to stdout
#   --results-dir <dir> Directory for results (default: ./netem-results)
#   --help              Show this help message
#
# Prerequisites:
#   - Docker cluster running (./scripts/cluster-up.sh)
#   - jq installed (for JSON parsing)
#   - Python 3 available (used by scenario scripts)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS_FILE="${SCRIPT_DIR}/scenarios.json"
RESULTS_DIR="./netem-results"
JSON_OUTPUT=false
RUN_ALL=false
SCENARIO_NAME=""

# --- Argument parsing ---

usage() {
    cat <<'USAGE'
Usage: ./scripts/netem/runner.sh [OPTIONS]

Options:
  --scenario <name>   Run a specific scenario by name
  --all               Run all scenarios
  --list              List available scenarios
  --json-output       Output results as JSON to stdout
  --results-dir <dir> Directory for results (default: ./netem-results)
  --help              Show this help message

Examples:
  ./scripts/netem/runner.sh --list
  ./scripts/netem/runner.sh --scenario partition-recovery
  ./scripts/netem/runner.sh --all --json-output
  ./scripts/netem/runner.sh --all --results-dir /tmp/netem-results
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario)
            SCENARIO_NAME="${2:?--scenario requires a name argument}"
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
        --json-output)
            JSON_OUTPUT=true
            shift
            ;;
        --results-dir)
            RESULTS_DIR="${2:?--results-dir requires a directory argument}"
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

# --- Prerequisites check ---

if [ ! -f "$SCENARIOS_FILE" ]; then
    echo "[ERROR] Scenarios file not found: ${SCENARIOS_FILE}" >&2
    exit 1
fi

# Check for jq
if ! command -v jq > /dev/null 2>&1; then
    echo "[ERROR] jq is required but not installed." >&2
    echo "  Install with: brew install jq (macOS) or apt-get install jq (Linux)" >&2
    exit 1
fi

# --- Scenario loading ---

# Get number of scenarios
SCENARIO_COUNT=$(jq '.scenarios | length' "$SCENARIOS_FILE")

# list_scenarios - print available scenarios in a table
list_scenarios() {
    echo "Available netem scenarios:"
    echo ""
    printf "  %-25s %-55s %s\n" "NAME" "DESCRIPTION" "TAGS"
    printf "  %-25s %-55s %s\n" "----" "-----------" "----"
    for i in $(seq 0 $(( SCENARIO_COUNT - 1 ))); do
        local name description tags
        name=$(jq -r ".scenarios[$i].name" "$SCENARIOS_FILE")
        description=$(jq -r ".scenarios[$i].description" "$SCENARIOS_FILE")
        tags=$(jq -r ".scenarios[$i].tags | join(\", \")" "$SCENARIOS_FILE")
        printf "  %-25s %-55s %s\n" "$name" "$description" "$tags"
    done
    echo ""
    echo "Total: ${SCENARIO_COUNT} scenario(s)"
}

# find_scenario <name> - prints the index of a scenario, or -1 if not found
find_scenario() {
    local target="$1"
    for i in $(seq 0 $(( SCENARIO_COUNT - 1 ))); do
        local name
        name=$(jq -r ".scenarios[$i].name" "$SCENARIOS_FILE")
        if [ "$name" = "$target" ]; then
            echo "$i"
            return 0
        fi
    done
    echo "-1"
    return 1
}

# --- List mode ---

if [ "${LIST_ONLY:-false}" = "true" ]; then
    list_scenarios
    exit 0
fi

# --- Validate selection ---

if [ "$RUN_ALL" = "false" ] && [ -z "$SCENARIO_NAME" ]; then
    echo "[ERROR] Specify --all, --scenario <name>, or --list." >&2
    echo "" >&2
    usage >&2
    exit 1
fi

# --- Docker cluster check ---

check_docker_cluster() {
    echo "[runner] Checking Docker cluster..."
    local running
    running=$(docker ps --filter "name=asteroidb-node" --format '{{.Names}}' 2>/dev/null | wc -l | tr -d ' ')
    if [ "$running" -lt 1 ]; then
        echo "[ERROR] No AsteroidDB containers running." >&2
        echo "  Start the cluster with: ./scripts/cluster-up.sh" >&2
        return 1
    fi
    echo "[runner] Found ${running} AsteroidDB container(s) running."
    return 0
}

if ! check_docker_cluster; then
    exit 1
fi

# --- Results directory ---

mkdir -p "$RESULTS_DIR"

# --- Run a single scenario ---

# run_scenario <index>
# Runs the scenario at the given index, captures output, and writes a JSON result.
# Returns the exit code of the scenario script.
run_scenario() {
    local idx="$1"
    local name description script timeout_seconds
    name=$(jq -r ".scenarios[$idx].name" "$SCENARIOS_FILE")
    description=$(jq -r ".scenarios[$idx].description" "$SCENARIOS_FILE")
    script=$(jq -r ".scenarios[$idx].script" "$SCENARIOS_FILE")
    timeout_seconds=$(jq -r ".scenarios[$idx].timeout_seconds" "$SCENARIOS_FILE")

    local script_path="${SCRIPT_DIR}/${script}"

    if [ ! -x "$script_path" ]; then
        echo "[ERROR] Scenario script not found or not executable: ${script_path}" >&2
        write_result "$name" "error" 0 "Script not found or not executable: ${script}"
        return 1
    fi

    echo ""
    echo "======================================================================"
    echo "[runner] Starting scenario: ${name}"
    echo "  Description: ${description}"
    echo "  Script:      ${script}"
    echo "  Timeout:     ${timeout_seconds}s"
    echo "======================================================================"
    echo ""

    local start_time end_time duration_seconds exit_code output
    start_time=$(date +%s)

    # Run with timeout; capture both stdout and stderr.
    local output_file="${RESULTS_DIR}/${name}-output.txt"
    set +e
    if command -v timeout > /dev/null 2>&1; then
        # GNU coreutils timeout (Linux)
        timeout "${timeout_seconds}" bash "$script_path" > "$output_file" 2>&1
        exit_code=$?
        if [ "$exit_code" -eq 124 ]; then
            echo "[runner] Scenario timed out after ${timeout_seconds}s" | tee -a "$output_file"
        fi
    elif command -v gtimeout > /dev/null 2>&1; then
        # GNU coreutils via Homebrew (macOS)
        gtimeout "${timeout_seconds}" bash "$script_path" > "$output_file" 2>&1
        exit_code=$?
        if [ "$exit_code" -eq 124 ]; then
            echo "[runner] Scenario timed out after ${timeout_seconds}s" | tee -a "$output_file"
        fi
    else
        # Fallback: no timeout enforcement, just run the script
        echo "[runner] WARNING: 'timeout' command not available; running without timeout enforcement."
        bash "$script_path" > "$output_file" 2>&1
        exit_code=$?
    fi
    set -e

    end_time=$(date +%s)
    duration_seconds=$(( end_time - start_time ))

    # Print captured output
    cat "$output_file"

    local status
    if [ "$exit_code" -eq 0 ]; then
        status="pass"
        echo ""
        echo "[runner] Scenario '${name}' PASSED in ${duration_seconds}s"
    elif [ "$exit_code" -eq 124 ]; then
        status="timeout"
        echo ""
        echo "[runner] Scenario '${name}' TIMED OUT after ${timeout_seconds}s"
    else
        status="fail"
        echo ""
        echo "[runner] Scenario '${name}' FAILED (exit code ${exit_code}) in ${duration_seconds}s"
    fi

    output=$(cat "$output_file")
    write_result "$name" "$status" "$duration_seconds" "$output"

    return "$exit_code"
}

# write_result <name> <status> <duration_seconds> <output>
# Writes a JSON result file.
write_result() {
    local name="$1"
    local status="$2"
    local duration_seconds="$3"
    local output="$4"
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    local result_file="${RESULTS_DIR}/${name}-result.json"

    # Use jq to safely encode the output string as JSON.
    jq -n \
        --arg timestamp "$timestamp" \
        --arg scenario "$name" \
        --arg status "$status" \
        --argjson duration "$duration_seconds" \
        --arg output "$output" \
        '{
            timestamp: $timestamp,
            scenario: $scenario,
            status: $status,
            duration_seconds: $duration,
            output: $output
        }' > "$result_file"

    if [ "$JSON_OUTPUT" = "true" ]; then
        cat "$result_file"
    fi
}

# --- Main execution ---

ALL_RESULTS=()
OVERALL_EXIT=0

if [ "$RUN_ALL" = "true" ]; then
    echo "[runner] Running all ${SCENARIO_COUNT} scenario(s)..."
    for i in $(seq 0 $(( SCENARIO_COUNT - 1 ))); do
        name=$(jq -r ".scenarios[$i].name" "$SCENARIOS_FILE")
        set +e
        run_scenario "$i"
        ec=$?
        set -e
        ALL_RESULTS+=("${name}:${ec}")
        if [ "$ec" -ne 0 ]; then
            OVERALL_EXIT=1
        fi
    done
else
    idx=$(find_scenario "$SCENARIO_NAME" || true)
    if [ "$idx" = "-1" ] || [ -z "$idx" ]; then
        echo "[ERROR] Scenario not found: ${SCENARIO_NAME}" >&2
        echo "" >&2
        list_scenarios >&2
        exit 1
    fi
    set +e
    run_scenario "$idx"
    ec=$?
    set -e
    ALL_RESULTS+=("${SCENARIO_NAME}:${ec}")
    if [ "$ec" -ne 0 ]; then
        OVERALL_EXIT=1
    fi
fi

# --- Summary ---

echo ""
echo "======================================================================"
echo "[runner] Summary"
echo "======================================================================"
for entry in "${ALL_RESULTS[@]}"; do
    name="${entry%%:*}"
    code="${entry##*:}"
    if [ "$code" -eq 0 ]; then
        echo "  PASS  ${name}"
    elif [ "$code" -eq 124 ]; then
        echo "  TIMEOUT  ${name}"
    else
        echo "  FAIL  ${name} (exit code ${code})"
    fi
done
echo ""
echo "Results saved to: ${RESULTS_DIR}/"
echo "======================================================================"

exit "$OVERALL_EXIT"
