#!/usr/bin/env bash
# Add network delay to a container using tc/netem.
#
# Usage: ./scripts/netem/add-delay.sh <container> <delay_ms>
# Example: ./scripts/netem/add-delay.sh asteroidb-node-3 200
#
# Prerequisites:
#   - The container must have NET_ADMIN capability (see docker-compose.yml).
#   - iproute2 must be available inside the container. The script will
#     attempt to install it via apt-get if the `tc` command is missing.
set -euo pipefail

CONTAINER="${1:?Usage: $0 <container> <delay_ms>}"
DELAY_MS="${2:?Usage: $0 <container> <delay_ms>}"

echo "[netem] Adding ${DELAY_MS}ms delay to ${CONTAINER} ..."

# Ensure tc is available inside the container.
if ! docker exec "$CONTAINER" which tc > /dev/null 2>&1; then
    echo "[netem] tc not found in ${CONTAINER}, installing iproute2 ..."
    docker exec "$CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq iproute2 > /dev/null 2>&1"
fi

# Remove any existing qdisc first (ignore errors if none exists).
docker exec "$CONTAINER" tc qdisc del dev eth0 root 2>/dev/null || true

# Add netem delay.
docker exec "$CONTAINER" tc qdisc add dev eth0 root netem delay "${DELAY_MS}ms"

echo "[netem] ${CONTAINER}: ${DELAY_MS}ms delay applied."
echo "[netem] Verify with: docker exec ${CONTAINER} tc qdisc show dev eth0"
