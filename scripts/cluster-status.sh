#!/usr/bin/env bash
set -euo pipefail

NODES=("localhost:3001" "localhost:3002" "localhost:3003")
NODE_NAMES=("node-1" "node-2" "node-3")

echo "AsteroidDB Cluster Status"
echo "========================="
echo ""

all_ok=true

for i in "${!NODES[@]}"; do
    addr="${NODES[$i]}"
    name="${NODE_NAMES[$i]}"

    if curl -sf --max-time 3 "http://${addr}/api/eventual/__health_check" > /dev/null 2>&1; then
        # The endpoint returns 200 with null value for non-existent keys,
        # which means the HTTP server is up and responding.
        echo "  ${name} (${addr}): UP"
    else
        # Even a 200 with {"key":"__health_check","value":null} counts as UP.
        # Try again and check HTTP status code.
        status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://${addr}/api/eventual/__health_check" 2>/dev/null || echo "000")
        if [ "$status" = "200" ]; then
            echo "  ${name} (${addr}): UP"
        else
            echo "  ${name} (${addr}): DOWN (HTTP ${status})"
            all_ok=false
        fi
    fi
done

echo ""
if $all_ok; then
    echo "All nodes are healthy."
else
    echo "Some nodes are not responding. Check 'docker compose logs' for details."
fi
