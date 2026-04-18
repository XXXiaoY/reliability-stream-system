"""
Fleet Simulator - Minimal smoke test version.
Sends simple vehicle telemetry events to Kafka.
"""

import json
import uuid
import time
import random
from datetime import datetime

from kafka import KafkaProducer


KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vehicle-telemetry"

# Simulate 3 vehicles
VEHICLE_IDS = ["vehicle-001", "vehicle-002", "vehicle-003"]

# Starting positions (roughly Seattle area)
VEHICLE_STATE = {
    "vehicle-001": {"lat": 47.6062, "lng": -122.3321},
    "vehicle-002": {"lat": 47.6205, "lng": -122.3493},
    "vehicle-003": {"lat": 47.6101, "lng": -122.3420},
}


def create_event(vehicle_id: str) -> dict:
    """Create a single location telemetry event."""
    state = VEHICLE_STATE[vehicle_id]

    # Simulate small movement
    state["lat"] += random.uniform(-0.001, 0.001)
    state["lng"] += random.uniform(-0.001, 0.001)

    now_ms = int(time.time() * 1000)

    return {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "event_time": now_ms,
        "ingest_time": now_ms,
        "schema_version": "1.0",
        "event_type": "LOCATION",
        "payload": {
            "latitude": round(state["lat"], 6),
            "longitude": round(state["lng"], 6),
            "altitude": round(random.uniform(0, 200), 1),
            "heading": round(random.uniform(0, 360), 1),
        },
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"Sending events to topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            vehicle_id = random.choice(VEHICLE_IDS)
            event = create_event(vehicle_id)

            producer.send(TOPIC, key=vehicle_id, value=event)
            count += 1

            ts = datetime.fromtimestamp(event["event_time"] / 1000).strftime("%H:%M:%S")
            print(f"[{count}] {vehicle_id} | {ts} | "
                  f"({event['payload']['latitude']}, {event['payload']['longitude']})")

            time.sleep(0.5)  # 2 events per second

    except KeyboardInterrupt:
        print(f"\nStopped. Sent {count} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
