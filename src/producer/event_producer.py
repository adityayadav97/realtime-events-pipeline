"""Kafka producer that simulates a real-time clickstream of user events.

Default throughput is 200 events/sec (\u2248 12,000 events/min) so the streaming
consumer is exercised at a realistic 10K+ events/min rate. Override with --rate
for a lighter local run, e.g. `python -m src.producer.event_producer --rate 50`.
"""

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
        linger_ms=20,
        batch_size=64 * 1024,
    )
    print(
        f"\U0001F680 Producing ~{rate} events/sec (\u2248 {rate * 60:,} events/min) "
        f"to topic '{KAFKA_TOPIC}' ... Ctrl+C to stop"
    )
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
        print(f"\n\U0001F6D1 stopped. total sent = {sent:,}")
    finally:
        producer.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200, help="events per second (default 200 \u2248 12K/min)")
    main(ap.parse_args().rate)
