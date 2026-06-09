"""Synthetic clickstream event generator.

Produces dictionaries whose keys match the ClickHouse destination table
columns 1:1, so ClickPipes can ingest them with the JSONEachRow format
without any field mapping.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

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


# Seconds between consecutive events within the same session (backfill mode).
_MIN_EVENT_GAP_S = 5
_MAX_EVENT_GAP_S = 300


class SessionPool:
    """Keeps a rolling set of active sessions so events look like real browsing
    sessions instead of fully random rows.

    Each session carries its own clock. In backfill mode (backfill_seconds > 0)
    a session starts at a random point within the window and its events advance
    the clock by small gaps, so a session's events stay clustered and ordered in
    time (which funnel/sequence/window queries rely on). In live mode events are
    stamped at the current time.
    """

    def __init__(self, size: int = 500, backfill_seconds: float = 0.0) -> None:
        self._size = size
        self._backfill_seconds = backfill_seconds
        self._sessions: list[dict] = []

    def _new_session(self) -> dict:
        now = datetime.now(timezone.utc)
        if self._backfill_seconds > 0:
            start = now - timedelta(seconds=random.uniform(0, self._backfill_seconds))
        else:
            start = now
        return {
            "user_id": random.randint(1, 50_000),
            "session_id": str(uuid.uuid4()),
            "clock": start,
        }

    def pick(self) -> tuple[int, str, datetime]:
        now = datetime.now(timezone.utc)
        if len(self._sessions) < self._size or random.random() < 0.1:
            session = self._new_session()
            self._sessions.append(session)
            if len(self._sessions) > self._size:
                self._sessions.pop(0)
        else:
            session = random.choice(self._sessions)
            if self._backfill_seconds > 0:
                session["clock"] += timedelta(
                    seconds=random.uniform(_MIN_EVENT_GAP_S, _MAX_EVENT_GAP_S)
                )

        if self._backfill_seconds > 0:
            # Never emit a timestamp in the future.
            event_dt = min(session["clock"], now)
        else:
            event_dt = now
        return session["user_id"], session["session_id"], event_dt


def make_event(pool: SessionPool) -> dict:
    """Build one event. event_time comes from the session's clock (see SessionPool)."""
    user_id, session_id, event_dt = pool.pick()
    event_type = random.choices(_event_names, weights=_event_weights, k=1)[0]
    price = 0.0
    if event_type in ("add_to_cart", "purchase"):
        price = round(random.uniform(9.99, 499.99), 2)

    return {
        "event_id": str(uuid.uuid4()),
        # ISO-8601 with milliseconds; ClickHouse parses this into DateTime64(3).
        "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
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
