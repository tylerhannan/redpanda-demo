"""Synthetic clickstream event generator.

Produces dictionaries whose keys match the ClickHouse destination table
columns 1:1, so ClickPipes can ingest them with the JSONEachRow format
without any field mapping.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

EVENT_TYPES = [
    ("page_view", 0.70),
    ("click", 0.18),
    ("add_to_cart", 0.08),
    ("purchase", 0.04),
]

DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "BR", "JP", "AU", "CA", "NL"]
PATHS = [
    "/",
    "/products",
    "/products/keyboard",
    "/products/monitor",
    "/products/headphones",
    "/cart",
    "/checkout",
    "/blog/clickhouse-tips",
    "/pricing",
    "/about",
]
REFERRERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://news.ycombinator.com/",
    "https://twitter.com/",
    "",  # direct
]

_event_names = [e for e, _ in EVENT_TYPES]
_event_weights = [w for _, w in EVENT_TYPES]


class SessionPool:
    """Keeps a rolling set of active (user_id, session_id) pairs so events
    look like real browsing sessions instead of fully random rows."""

    def __init__(self, size: int = 500) -> None:
        self._size = size
        self._sessions: list[tuple[int, str]] = []

    def pick(self) -> tuple[int, str]:
        if len(self._sessions) < self._size or random.random() < 0.1:
            session = (random.randint(1, 50_000), str(uuid.uuid4()))
            self._sessions.append(session)
            if len(self._sessions) > self._size:
                self._sessions.pop(0)
            return session
        return random.choice(self._sessions)


def make_event(pool: SessionPool) -> dict:
    user_id, session_id = pool.pick()
    event_type = random.choices(_event_names, weights=_event_weights, k=1)[0]
    price = 0.0
    if event_type in ("add_to_cart", "purchase"):
        price = round(random.uniform(9.99, 499.99), 2)

    return {
        "event_id": str(uuid.uuid4()),
        # ISO-8601 with milliseconds; ClickHouse parses this into DateTime64(3).
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "url": random.choice(PATHS),
        "referrer": random.choice(REFERRERS),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "country": random.choice(COUNTRIES),
        "price": price,
    }
