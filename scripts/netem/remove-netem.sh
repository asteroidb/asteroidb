#!/usr/bin/env bash
# Remove all netem rules from a container, restoring normal networking.
#
# Usage: ./scripts/netem/remove-netem.sh <container>
# Example: ./scripts/netem/remove-netem.sh asteroidb-node-3
set -euo pipefail

CONTAINER="${1:?Usage: $0 <container>}"

echo "[netem] Removing netem rules from ${CONTAINER} ..."

# Delete the root qdisc. If no netem qdisc exists, tc will return an
# error which we silently ignore.
if docker exec "$CONTAINER" tc qdisc del dev eth0 root 2>/dev/null; then
    echo "[netem] ${CONTAINER}: netem rules removed. Network restored."
else
    echo "[netem] ${CONTAINER}: no netem rules found (already clean)."
fi
