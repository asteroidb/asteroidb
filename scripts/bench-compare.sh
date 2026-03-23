#!/usr/bin/env bash
# bench-compare.sh -- Compare two sets of Criterion benchmark results and flag regressions.
#
# Usage:
#   bash scripts/bench-compare.sh <baseline-criterion-dir> <current-criterion-dir>
#
# Reads Criterion's estimates.json files from both directories, extracts the
# point estimate for each benchmark, computes the percentage change, and flags
# any benchmark that regressed by more than THRESHOLD (default 10%).
#
# Exit codes:
#   0 -- no regressions found
#   1 -- at least one regression found
#   2 -- usage error

set -euo pipefail

THRESHOLD="${BENCH_REGRESSION_THRESHOLD:-10}"

if [ $# -ne 2 ]; then
    echo "Usage: $0 <baseline-criterion-dir> <current-criterion-dir>" >&2
    exit 2
fi

BASELINE_DIR="$1"
CURRENT_DIR="$2"

if [ ! -d "$BASELINE_DIR" ]; then
    echo "Error: baseline directory not found: $BASELINE_DIR" >&2
    exit 2
fi

if [ ! -d "$CURRENT_DIR" ]; then
    echo "Error: current directory not found: $CURRENT_DIR" >&2
    exit 2
fi

has_regressions=false
count_total=0
count_improved=0
count_regressed=0
count_unchanged=0

# Collect results into arrays for table formatting
declare -a bench_names=()
declare -a bench_baselines=()
declare -a bench_currents=()
declare -a bench_changes=()
declare -a bench_statuses=()

# extract_point_estimate <estimates.json>
# Prints the point estimate in nanoseconds from a Criterion estimates.json file.
extract_point_estimate() {
    local file="$1"
    if [ ! -f "$file" ]; then
        echo ""
        return
    fi
    # Criterion stores: { "mean": { "point_estimate": <ns>, ... }, ... }
    # We use the mean point_estimate.
    python3 -c "
import json, sys
with open('$file') as f:
    data = json.load(f)
print(data.get('mean', data.get('Mean', {})).get('point_estimate', data.get('mean', data.get('Mean', {})).get('point_estimate', '')))
" 2>/dev/null || echo ""
}

# format_time <nanoseconds>
# Prints a human-readable duration string.
format_time() {
    local ns="$1"
    python3 -c "
ns = float('$ns')
if ns < 1000:
    print(f'{ns:.1f} ns')
elif ns < 1_000_000:
    print(f'{ns/1000:.2f} us')
elif ns < 1_000_000_000:
    print(f'{ns/1_000_000:.2f} ms')
else:
    print(f'{ns/1_000_000_000:.2f} s')
" 2>/dev/null || echo "${ns} ns"
}

# Find all benchmarks that exist in both baseline and current
while IFS= read -r estimates_file; do
    # Get the relative path from the current criterion dir
    rel_path="${estimates_file#"$CURRENT_DIR"/}"
    bench_dir=$(dirname "$rel_path")

    # Check if baseline has the same benchmark
    baseline_file="${BASELINE_DIR}/${rel_path}"
    if [ ! -f "$baseline_file" ]; then
        continue
    fi

    baseline_ns=$(extract_point_estimate "$baseline_file")
    current_ns=$(extract_point_estimate "$estimates_file")

    if [ -z "$baseline_ns" ] || [ -z "$current_ns" ]; then
        continue
    fi

    # Compute percentage change
    change_info=$(python3 -c "
baseline = float('$baseline_ns')
current = float('$current_ns')
if baseline == 0:
    print('0.0 UNCHANGED')
else:
    pct = ((current - baseline) / baseline) * 100
    if pct > $THRESHOLD:
        print(f'{pct:.1f} REGRESSION')
    elif pct < -$THRESHOLD:
        print(f'{pct:.1f} IMPROVED')
    else:
        print(f'{pct:.1f} UNCHANGED')
" 2>/dev/null || echo "0.0 UNCHANGED")

    pct_change=$(echo "$change_info" | awk '{print $1}')
    status=$(echo "$change_info" | awk '{print $2}')

    # Clean up benchmark name: remove /new/estimates.json path suffix
    bench_name="${bench_dir%/new}"
    bench_name="${bench_name%/base}"

    bench_names+=("$bench_name")
    bench_baselines+=("$(format_time "$baseline_ns")")
    bench_currents+=("$(format_time "$current_ns")")
    bench_changes+=("${pct_change}%")
    bench_statuses+=("$status")

    count_total=$((count_total + 1))
    case "$status" in
        REGRESSION) count_regressed=$((count_regressed + 1)); has_regressions=true ;;
        IMPROVED) count_improved=$((count_improved + 1)) ;;
        UNCHANGED) count_unchanged=$((count_unchanged + 1)) ;;
    esac

done < <(find "$CURRENT_DIR" -name "estimates.json" -path "*/new/*" 2>/dev/null | sort)

# Print results
echo ""
echo "### Summary"
echo ""
echo "- **Total benchmarks compared**: ${count_total}"
echo "- **Regressions** (>${THRESHOLD}% slower): ${count_regressed}"
echo "- **Improvements** (>${THRESHOLD}% faster): ${count_improved}"
echo "- **Unchanged**: ${count_unchanged}"
echo ""

if [ "$count_total" -eq 0 ]; then
    echo "No benchmarks found to compare."
    exit 0
fi

# Print regressions first if any
if [ "$count_regressed" -gt 0 ]; then
    echo "### REGRESSION Detected"
    echo ""
    echo "| Benchmark | Baseline | Current | Change | Status |"
    echo "|-----------|----------|---------|--------|--------|"
    for i in "${!bench_names[@]}"; do
        if [ "${bench_statuses[$i]}" = "REGRESSION" ]; then
            echo "| ${bench_names[$i]} | ${bench_baselines[$i]} | ${bench_currents[$i]} | ${bench_changes[$i]} | :red_circle: REGRESSION |"
        fi
    done
    echo ""
fi

# Full table
echo "### All Benchmarks"
echo ""
echo "| Benchmark | Baseline | Current | Change | Status |"
echo "|-----------|----------|---------|--------|--------|"
for i in "${!bench_names[@]}"; do
    case "${bench_statuses[$i]}" in
        REGRESSION) icon=":red_circle: REGRESSION" ;;
        IMPROVED) icon=":green_circle: Improved" ;;
        *) icon=":white_circle: Unchanged" ;;
    esac
    echo "| ${bench_names[$i]} | ${bench_baselines[$i]} | ${bench_currents[$i]} | ${bench_changes[$i]} | ${icon} |"
done

if [ "$has_regressions" = true ]; then
    exit 1
else
    exit 0
fi
