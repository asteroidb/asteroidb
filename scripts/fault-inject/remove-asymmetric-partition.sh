#!/usr/bin/env bash
# Remove an asymmetric partition created by asymmetric-partition.sh.
#
# Usage: ./scripts/fault-inject/remove-asymmetric-partition.sh <from_container> <to_container>
# Example: ./scripts/fault-inject/remove-asymmetric-partition.sh asteroidb-node-1 asteroidb-node-2
set -euo pipefail

FROM_CONTAINER="${1:?Usage: $0 <from_container> <to_container>}"
TO_CONTAINER="${2:?Usage: $0 <from_container> <to_container>}"

echo "[asymmetric] Removing one-way block: ${FROM_CONTAINER} -> ${TO_CONTAINER}"

# Resolve the IP address of the target container.
TO_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$TO_CONTAINER")
if [ -z "$TO_IP" ]; then
    echo "[asymmetric] WARNING: Could not resolve IP for ${TO_CONTAINER}, flushing all OUTPUT rules."
    docker exec "$FROM_CONTAINER" iptables -F OUTPUT 2>/dev/null || true
    echo "[asymmetric] Flushed OUTPUT chain on ${FROM_CONTAINER}."
    exit 0
fi

# Remove the specific iptables rule.
if docker exec "$FROM_CONTAINER" iptables -D OUTPUT -d "$TO_IP" -j DROP 2>/dev/null; then
    echo "[asymmetric] Removed block: ${FROM_CONTAINER} -/-> ${TO_CONTAINER} (${TO_IP})"
else
    echo "[asymmetric] No matching rule found (already clean)."
fi
