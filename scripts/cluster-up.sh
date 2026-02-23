#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Starting AsteroidDB 3-node cluster..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --build

echo ""
echo "Cluster started. Nodes:"
echo "  node-1: http://localhost:3001"
echo "  node-2: http://localhost:3002"
echo "  node-3: http://localhost:3003"
echo ""
echo "Run 'scripts/cluster-status.sh' to check health."
echo "Run 'scripts/cluster-down.sh' to stop."
