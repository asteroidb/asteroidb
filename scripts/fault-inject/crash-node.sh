#!/usr/bin/env bash
# Crash (stop) and restart a Docker container, then verify health.
#
# Usage: ./scripts/fault-inject/crash-node.sh <container> [stop|start|restart]
#
# Actions:
#   stop    - docker stop the container (simulates crash)
#   start   - docker start the container and wait for health
#   restart - stop then start (default)
#
# Prerequisites:
#   - Docker available
#   - Container exists
set -euo pipefail

CONTAINER="${1:?Usage: $0 <container> [stop|start|restart]}"
ACTION="${2:-restart}"

HEALTH_RETRIES=20
HEALTH_INTERVAL=1

# Map container name to host port for health checks.
container_port() {
    case "$1" in
        asteroidb-node-1) echo 3001 ;;
        asteroidb-node-2) echo 3002 ;;
        asteroidb-node-3) echo 3003 ;;
        *) echo "Unknown container: $1" >&2; return 1 ;;
    esac
}

wait_for_health() {
    local port
    port=$(container_port "$CONTAINER")
    local url="http://localhost:${port}/api/eventual/__health_check"

    echo "[crash-node] Waiting for ${CONTAINER} to become healthy..."
    for attempt in $(seq 1 "$HEALTH_RETRIES"); do
        if curl -sf --max-time 2 "$url" > /dev/null 2>&1; then
            echo "[crash-node] ${CONTAINER} is healthy (attempt ${attempt}/${HEALTH_RETRIES})."
            return 0
        fi
        sleep "$HEALTH_INTERVAL"
    done
    echo "[crash-node] ERROR: ${CONTAINER} did not become healthy after ${HEALTH_RETRIES} attempts." >&2
    return 1
}

case "$ACTION" in
    stop)
        echo "[crash-node] Stopping ${CONTAINER}..."
        docker stop "$CONTAINER"
        echo "[crash-node] ${CONTAINER} stopped."
        ;;
    start)
        echo "[crash-node] Starting ${CONTAINER}..."
        docker start "$CONTAINER"
        wait_for_health
        ;;
    restart)
        echo "[crash-node] Restarting ${CONTAINER} (stop + start)..."
        docker stop "$CONTAINER"
        echo "[crash-node] ${CONTAINER} stopped. Waiting 2s before restart..."
        sleep 2
        docker start "$CONTAINER"
        wait_for_health
        ;;
    *)
        echo "Unknown action: ${ACTION}" >&2
        echo "Usage: $0 <container> [stop|start|restart]" >&2
        exit 1
        ;;
esac
