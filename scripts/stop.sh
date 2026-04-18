#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infra"

echo "=== Stopping infrastructure ==="
docker compose -f "$INFRA_DIR/docker-compose.yml" down

echo "=== Done ==="
