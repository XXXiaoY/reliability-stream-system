#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PASS=0
FAIL=0

echo "============================================"
echo "  CHAOS TEST SUITE"
echo "  Vehicle Telemetry Stream Processing System"
echo "============================================"
echo ""

check() {
    local name="$1"
    local condition="$2"
    if eval "$condition"; then
        echo "  [PASS] $name"
        PASS=$((PASS + 1))
    else
        echo "  [FAIL] $name"
        FAIL=$((FAIL + 1))
    fi
}

query_prom_sum() {
    local query="$1"
    curl -s "http://localhost:9090/api/v1/query?query=${query}" | python3 -c '
import json
import sys

try:
    results = json.load(sys.stdin)["data"]["result"]
    print(int(sum(float(item["value"][1]) for item in results)) if results else 0)
except Exception:
    print(0)
'
}

cancel_running_jobs() {
    local job_ids
    job_ids=$(docker exec flink-jobmanager flink list -r 2>&1 | grep -Eo '[a-f0-9]{32}' || true)
    for job_id in $job_ids; do
        docker exec flink-jobmanager flink cancel "$job_id" > /dev/null 2>&1 || true
    done
}

recreate_topic() {
    local topic="$1"
    local partitions="$2"

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:29092 \
        --delete \
        --topic "$topic" > /dev/null 2>&1 || true

    sleep 2

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:29092 \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 > /dev/null 2>&1
}

echo "[1/5] Preparing test environment..."
docker exec -i postgres psql -U telemetry -d telemetry -c "TRUNCATE telemetry_events;" > /dev/null 2>&1

cancel_running_jobs
sleep 3

recreate_topic "vehicle-telemetry" 4
recreate_topic "vehicle-telemetry-dlq" 2
recreate_topic "late-events" 2

docker cp "$REPO_ROOT/flink-job/target/flink-job-1.0-SNAPSHOT.jar" flink-jobmanager:/tmp/ > /dev/null 2>&1
docker exec flink-jobmanager flink run -d /tmp/flink-job-1.0-SNAPSHOT.jar > /dev/null 2>&1
sleep 5

echo "  Environment ready."
echo ""

echo "[2/5] Test: Normal event processing..."
cd "$REPO_ROOT/fleet-simulator"
python3 - <<'PY' 2>/dev/null
import json
import random
import time
import uuid

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

for i in range(100):
    now_ms = int(time.time() * 1000)
    event = {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": f"vehicle-{(i % 3) + 1:03d}",
        "event_time": now_ms,
        "ingest_time": now_ms,
        "schema_version": "1.0",
        "event_type": "LOCATION",
        "payload": {
            "latitude": 47.6 + random.uniform(-0.01, 0.01),
            "longitude": -122.3 + random.uniform(-0.01, 0.01),
            "altitude": random.uniform(0, 200),
            "heading": random.uniform(0, 360),
        },
    }
    producer.send("vehicle-telemetry", key=event["vehicle_id"], value=event)
    time.sleep(0.05)

producer.flush()
producer.close()
PY

sleep 10
PROCESSED=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM telemetry_events;" | tr -d ' ')
check "100 events sent -> $PROCESSED stored in PostgreSQL" "[ $PROCESSED -ge 95 ]"
echo ""

echo "[3/5] Test: Duplicate event rejection..."
BEFORE=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM telemetry_events;" | tr -d ' ')

cd "$REPO_ROOT/fleet-simulator"
python3 - <<'PY' 2>/dev/null
import json
import time
import uuid

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

fixed_id = str(uuid.uuid4())
for _ in range(20):
    now_ms = int(time.time() * 1000)
    event = {
        "event_id": fixed_id,
        "vehicle_id": "vehicle-001",
        "event_time": now_ms,
        "ingest_time": now_ms,
        "schema_version": "1.0",
        "event_type": "LOCATION",
        "payload": {
            "latitude": 47.6,
            "longitude": -122.3,
            "altitude": 100,
            "heading": 90,
        },
    }
    producer.send("vehicle-telemetry", key="vehicle-001", value=event)
    time.sleep(0.05)

producer.flush()
producer.close()
PY

sleep 8
AFTER=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM telemetry_events;" | tr -d ' ')
NEW_ROWS=$((AFTER - BEFORE))
check "20 duplicates sent -> only $NEW_ROWS new row(s) in PostgreSQL (expect 1)" "[ $NEW_ROWS -le 1 ]"

DUP_METRIC=$(query_prom_sum "flink_taskmanager_job_task_operator_telemetry_events_duplicate")
check "Prometheus duplicate counter > 0 (current: $DUP_METRIC)" "[ ${DUP_METRIC:-0} -gt 0 ]"
echo ""

echo "[4/5] Test: Malformed events routed to DLQ..."
cd "$REPO_ROOT/fleet-simulator"
python3 - <<'PY' 2>/dev/null
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
for _ in range(5):
    producer.send("vehicle-telemetry", key=b"bad", value=b"{broken json!!}")

producer.flush()
producer.close()
PY

sleep 8
DLQ_MESSAGES=$(docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic vehicle-telemetry-dlq --from-beginning --timeout-ms 5000 2>/dev/null || true)
DLQ_COUNT=$(printf "%s\n" "$DLQ_MESSAGES" | sed '/^$/d' | wc -l | tr -d ' ')
check "5 malformed events -> $DLQ_COUNT message(s) in DLQ topic (expect >= 5)" "[ $DLQ_COUNT -ge 5 ]"

DLQ_METRIC=$(query_prom_sum "flink_taskmanager_job_task_operator_telemetry_events_dlq")
check "Prometheus DLQ counter > 0 (current: $DLQ_METRIC)" "[ ${DLQ_METRIC:-0} -gt 0 ]"
echo ""

echo "[5/5] Test: TaskManager crash recovery..."
BEFORE_CRASH=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM telemetry_events;" | tr -d ' ')

docker restart flink-taskmanager > /dev/null 2>&1
echo "  TaskManager restarted. Waiting for recovery..."
sleep 20

cd "$REPO_ROOT/fleet-simulator"
python3 - <<'PY' 2>/dev/null
import json
import random
import time
import uuid

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

for i in range(50):
    now_ms = int(time.time() * 1000)
    event = {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": f"vehicle-{(i % 3) + 1:03d}",
        "event_time": now_ms,
        "ingest_time": now_ms,
        "schema_version": "1.0",
        "event_type": "LOCATION",
        "payload": {
            "latitude": 47.6 + random.uniform(-0.01, 0.01),
            "longitude": -122.3 + random.uniform(-0.01, 0.01),
            "altitude": random.uniform(0, 200),
            "heading": random.uniform(0, 360),
        },
    }
    producer.send("vehicle-telemetry", key=event["vehicle_id"], value=event)
    time.sleep(0.1)

producer.flush()
producer.close()
PY

sleep 15
AFTER_CRASH=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM telemetry_events;" | tr -d ' ')
RECOVERED=$((AFTER_CRASH - BEFORE_CRASH))
check "Post-crash: $RECOVERED new events processed after TaskManager restart (expect >= 40)" "[ $RECOVERED -ge 40 ]"

DUP_ROWS=$(docker exec postgres psql -U telemetry -d telemetry -t -c "SELECT count(*) FROM (SELECT event_id FROM telemetry_events GROUP BY event_id HAVING count(*) > 1) x;" | tr -d ' ')
check "No duplicate rows in PostgreSQL after crash recovery (found: $DUP_ROWS)" "[ $DUP_ROWS -eq 0 ]"
echo ""

echo "============================================"
echo "  RESULTS: $PASS passed, $FAIL failed"
echo "============================================"

if [ $FAIL -eq 0 ]; then
    echo "  ALL TESTS PASSED"
else
    echo "  SOME TESTS FAILED - investigate above"
fi
