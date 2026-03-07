#!/usr/bin/env bash
# Create an asymmetric network partition: block traffic FROM one container TO another.
#
# Usage: ./scripts/fault-inject/asymmetric-partition.sh <from_container> <to_container>
# Example: ./scripts/fault-inject/asymmetric-partition.sh asteroidb-node-1 asteroidb-node-2
#
# This blocks outbound traffic from <from_container> destined for <to_container>,
# but allows traffic in the reverse direction (to -> from).
#
# Uses tc + iptables inside the container to achieve one-way blocking.
#
# Prerequisites:
#   - Containers must have NET_ADMIN capability
#   - iptables and iproute2 must be available inside the container
set -euo pipefail

FROM_CONTAINER="${1:?Usage: $0 <from_container> <to_container>}"
TO_CONTAINER="${2:?Usage: $0 <from_container> <to_container>}"

echo "[asymmetric] Setting up one-way block: ${FROM_CONTAINER} -> ${TO_CONTAINER}"

# Resolve the IP address of the target container.
TO_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$TO_CONTAINER")
if [ -z "$TO_IP" ]; then
    echo "[asymmetric] ERROR: Could not resolve IP for ${TO_CONTAINER}" >&2
    exit 1
fi
echo "[asymmetric] Target IP: ${TO_IP}"

# Ensure iptables is available inside the source container.
if ! docker exec "$FROM_CONTAINER" which iptables > /dev/null 2>&1; then
    echo "[asymmetric] iptables not found in ${FROM_CONTAINER}, installing..."
    docker exec "$FROM_CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq iptables > /dev/null 2>&1"
fi

# Add an iptables OUTPUT rule to drop all packets to the target IP.
docker exec "$FROM_CONTAINER" iptables -A OUTPUT -d "$TO_IP" -j DROP

echo "[asymmetric] Blocked: ${FROM_CONTAINER} -> ${TO_CONTAINER} (${TO_IP})"
echo "[asymmetric] Traffic from ${TO_CONTAINER} -> ${FROM_CONTAINER} is still allowed."
