"""Produce synthetic clickstream events to a Redpanda Serverless topic.

Usage:
    python produce.py                 # uses .env settings
    python produce.py --count 5000    # override number of events
    python produce.py --rate 500      # override events/second
    python produce.py --count 0       # run forever (Ctrl-C to stop)

The events are JSON-encoded, one object per Kafka message, which lines up
with the ClickPipes "JSONEachRow" ingestion format.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

from events import SessionPool, make_event

load_dotenv()

_stop = False


def _handle_sigint(_signum, _frame) -> None:
    global _stop
    _stop = True
    print("\nStopping after current batch flushes...", file=sys.stderr)


def build_producer() -> Producer:
    brokers = os.environ.get("REDPANDA_BROKERS")
    username = os.environ.get("REDPANDA_USERNAME")
    password = os.environ.get("REDPANDA_PASSWORD")
    mechanism = os.environ.get("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")

    missing = [
        name
        for name, val in (
            ("REDPANDA_BROKERS", brokers),
            ("REDPANDA_USERNAME", username),
            ("REDPANDA_PASSWORD", password),
        )
        if not val
    ]
    if missing:
        sys.exit(f"Missing required env vars: {', '.join(missing)}. Copy .env.example to .env and fill it in.")

    return Producer(
        {
            "bootstrap.servers": brokers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": mechanism,
            "sasl.username": username,
            "sasl.password": password,
            # Reasonable batching for throughput.
            "linger.ms": 50,
            "compression.type": "lz4",
            "acks": "all",
            "client.id": "redpanda-clickhouse-demo-producer",
        }
    )


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=int(os.environ.get("EVENT_COUNT", "100000")))
    parser.add_argument("--rate", type=int, default=int(os.environ.get("EVENTS_PER_SECOND", "200")))
    parser.add_argument("--topic", default=os.environ.get("REDPANDA_TOPIC", "clickstream_events"))
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)

    producer = build_producer()
    pool = SessionPool()

    target = args.count
    rate = max(1, args.rate)
    interval = 1.0 / rate
    forever = target <= 0

    print(
        f"Producing {'unlimited' if forever else target} events to topic "
        f"'{args.topic}' at ~{rate}/s ...",
        file=sys.stderr,
    )

    sent = 0
    next_tick = time.perf_counter()
    try:
        while not _stop and (forever or sent < target):
            event = make_event(pool)
            producer.produce(
                topic=args.topic,
                key=event["session_id"],
                value=json.dumps(event),
                on_delivery=_delivery_report,
            )
            sent += 1

            # Serve librdkafka's background callbacks.
            producer.poll(0)

            if sent % 1000 == 0:
                print(f"  sent {sent} events", file=sys.stderr)

            # Simple rate limiting.
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        print("Flushing...", file=sys.stderr)
        producer.flush(30)
        print(f"Done. Total events sent: {sent}", file=sys.stderr)


if __name__ == "__main__":
    main()
