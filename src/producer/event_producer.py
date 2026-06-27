"""Kafka producer that simulates a real-time clickstream of user events."""

from __future__ import annotations
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

import sys
sys.path.append(".")
from src.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, EVENT_TYPES


def make_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10_000),
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "page": random.choice(["/home", "/search", "/product", "/cart", "/checkout"]),
        "amount": round(random.uniform(0, 500), 2),
    }


def main(rate: int):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"🚀 Producing ~{rate} events/sec to topic '{KAFKA_TOPIC}' ... Ctrl+C to stop")
    sent = 0
    try:
        while True:
            for _ in range(rate):
                producer.send(KAFKA_TOPIC, make_event())
            sent += rate
            print(f"  sent {sent:,} events", end="\r")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n🛑 stopped. total sent = {sent:,}")
    finally:
        producer.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=50, help="events per second")
    main(ap.parse_args().rate)
