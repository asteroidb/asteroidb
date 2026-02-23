#!/usr/bin/env bash
# Partition a container by dropping 100% of packets using tc/netem.
#
# Usage: ./scripts/netem/add-partition.sh <container>
# Example: ./scripts/netem/add-partition.sh asteroidb-node-3
#
# This simulates a complete network partition: the container can still
# run locally but cannot communicate with any other node.
#
# Prerequisites:
#   - The container must have NET_ADMIN capability (see docker-compose.yml).
#   - iproute2 must be available inside the container.
set -euo pipefail

CONTAINER="${1:?Usage: $0 <container>}"

echo "[netem] Partitioning ${CONTAINER} (100% packet loss) ..."

# Ensure tc is available inside the container.
if ! docker exec "$CONTAINER" which tc > /dev/null 2>&1; then
    echo "[netem] tc not found in ${CONTAINER}, installing iproute2 ..."
    docker exec "$CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq iproute2 > /dev/null 2>&1"
fi

# Remove any existing qdisc first (ignore errors if none exists).
docker exec "$CONTAINER" tc qdisc del dev eth0 root 2>/dev/null || true

# Add 100% packet loss to simulate full partition.
docker exec "$CONTAINER" tc qdisc add dev eth0 root netem loss 100%

echo "[netem] ${CONTAINER}: fully partitioned (100% packet loss)."
echo "[netem] Verify with: docker exec ${CONTAINER} tc qdisc show dev eth0"
