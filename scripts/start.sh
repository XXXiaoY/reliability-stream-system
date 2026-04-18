#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infra"

echo "=== Starting infrastructure ==="
docker compose -f "$INFRA_DIR/docker-compose.yml" up -d

echo "=== Waiting for Kafka to be ready ==="
sleep 10

echo "=== Creating Kafka topics ==="
docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic vehicle-telemetry --partitions 4 --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic vehicle-telemetry-dlq --partitions 2 --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic late-events --partitions 2 --replication-factor 1 \
  --if-not-exists

echo "=== Listing topics ==="
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

echo ""
echo "=== Infrastructure ready ==="
echo "Flink UI:    http://localhost:8081"
echo "Kafka:       localhost:9092"
echo "PostgreSQL:  localhost:5432 (telemetry/telemetry/telemetry123)"
