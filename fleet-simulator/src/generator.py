"""
Fleet Simulator - with fault injection.
Sends telemetry events with configurable disorder, duplication, and late arrival.
"""

import json
import uuid
import time
import random
import copy
from datetime import datetime

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vehicle-telemetry"

VEHICLE_IDS = ["vehicle-001", "vehicle-002", "vehicle-003"]

VEHICLE_STATE = {
    "vehicle-001": {"lat": 47.6062, "lng": -122.3321},
    "vehicle-002": {"lat": 47.6205, "lng": -122.3493},
    "vehicle-003": {"lat": 47.6101, "lng": -122.3420},
}

# --- Fault injection config ---
FAULT_CONFIG = {
    "duplicate_rate": 0.05,         # 5% of events sent twice
    "out_of_order_rate": 0.10,      # 10% of events delayed by a random buffer
    "out_of_order_max_delay": 10,   # max events to hold before releasing
    "late_event_rate": 0.03,        # 3% of events get event_time pushed back 60-120s
    "late_event_min_sec": 60,
    "late_event_max_sec": 120,
    "malformed_rate": 0.02,         # 2% of events are malformed JSON
}

# Stats
stats = {
    "total_sent": 0,
    "duplicates_injected": 0,
    "out_of_order_injected": 0,
    "late_events_injected": 0,
    "malformed_injected": 0,
}


def create_event(vehicle_id: str) -> dict:
    state = VEHICLE_STATE[vehicle_id]
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


def inject_late_event(event: dict) -> dict:
    """Push event_time back to simulate a late-arriving event."""
    delay_sec = random.uniform(
        FAULT_CONFIG["late_event_min_sec"],
        FAULT_CONFIG["late_event_max_sec"]
    )
    event["event_time"] = event["event_time"] - int(delay_sec * 1000)
    return event


def send_event(producer, event: dict, label: str = ""):
    tag = f" [{label}]" if label else ""
    producer.send(TOPIC, key=event["vehicle_id"], value=event)
    stats["total_sent"] += 1

    ts = datetime.fromtimestamp(event["event_time"] / 1000).strftime("%H:%M:%S")
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{stats['total_sent']}]{tag} {event['vehicle_id']} | "
          f"event_time={ts} wall={now} | "
          f"({event['payload']['latitude']}, {event['payload']['longitude']})")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"Fault config: {json.dumps(FAULT_CONFIG, indent=2)}")
    print("Press Ctrl+C to stop.\n")

    # Buffer for out-of-order injection
    ooo_buffer = []

    count = 0
    try:
        while True:
            vehicle_id = random.choice(VEHICLE_IDS)
            event = create_event(vehicle_id)
            count += 1

            # --- Fault: Malformed event ---
            if random.random() < FAULT_CONFIG["malformed_rate"]:
                malformed = '{"broken json": ' + str(random.randint(0, 999))
                producer.send(TOPIC, key="malformed", value=malformed)
                stats["total_sent"] += 1
                stats["malformed_injected"] += 1
                print(f"[{stats['total_sent']}] [MALFORMED] sent broken JSON")
                continue

            # --- Fault: Late event (mutate event_time to the past) ---
            if random.random() < FAULT_CONFIG["late_event_rate"]:
                event = inject_late_event(event)
                stats["late_events_injected"] += 1
                send_event(producer, event, "LATE")

            # --- Fault: Out-of-order (buffer and release later) ---
            elif random.random() < FAULT_CONFIG["out_of_order_rate"]:
                ooo_buffer.append(event)
                stats["out_of_order_injected"] += 1
                if len(ooo_buffer) >= FAULT_CONFIG["out_of_order_max_delay"]:
                    random.shuffle(ooo_buffer)
                    for buffered in ooo_buffer:
                        send_event(producer, buffered, "OOO")
                    ooo_buffer.clear()

            else:
                send_event(producer, event)

            # --- Fault: Duplicate (send same event again) ---
            if random.random() < FAULT_CONFIG["duplicate_rate"]:
                dup = copy.deepcopy(event)
                dup["ingest_time"] = int(time.time() * 1000)  # new ingest time
                stats["duplicates_injected"] += 1
                send_event(producer, dup, "DUP")

            time.sleep(0.5)

    except KeyboardInterrupt:
        # Flush remaining OOO buffer
        for buffered in ooo_buffer:
            send_event(producer, buffered, "OOO-FLUSH")
        producer.flush()
        producer.close()

        print(f"\n=== Generator Stats ===")
        print(f"Events generated:        {count}")
        print(f"Total sent (inc faults): {stats['total_sent']}")
        print(f"Duplicates injected:     {stats['duplicates_injected']}")
        print(f"Out-of-order injected:   {stats['out_of_order_injected']}")
        print(f"Late events injected:    {stats['late_events_injected']}")
        print(f"Malformed injected:      {stats['malformed_injected']}")


if __name__ == "__main__":
    main()
