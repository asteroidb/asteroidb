#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Stopping AsteroidDB cluster..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" down

echo "Cluster stopped."
