"""Create the Redpanda topic without rpk, using the confluent_kafka admin client.

Reads connection settings from the repo-root .env and creates REDPANDA_TOPIC.
Run it with the producer virtualenv so the dependencies are available:

    producer/.venv/bin/python scripts/setup_topic.py

(or activate the venv first, then `python scripts/setup_topic.py`).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def main() -> None:
    brokers = os.environ.get("REDPANDA_BROKERS")
    username = os.environ.get("REDPANDA_USERNAME")
    password = os.environ.get("REDPANDA_PASSWORD")
    if not (brokers and username and password):
        sys.exit("Set REDPANDA_BROKERS, REDPANDA_USERNAME, REDPANDA_PASSWORD in .env first.")

    topic = os.environ.get("REDPANDA_TOPIC", "clickstream_events")
    mechanism = os.environ.get("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
    partitions = int(os.environ.get("REDPANDA_PARTITIONS", "6"))

    admin = AdminClient(
        {
            "bootstrap.servers": brokers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": mechanism,
            "sasl.username": username,
            "sasl.password": password,
        }
    )

    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=3)])
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"Created topic: {name} ({partitions} partitions)")
        except Exception as exc:  # noqa: BLE001 - report any creation error, incl. "already exists"
            print(f"Topic {name}: {exc}")

    md = admin.list_topics(timeout=15)
    print("Topics now:", list(md.topics.keys()))


if __name__ == "__main__":
    main()
