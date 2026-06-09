"""Synthetic clickstream event generator.

Produces dictionaries whose keys match the ClickHouse destination table
columns 1:1, so ClickPipes can ingest them with the JSONEachRow format
without any field mapping.

The data carries deliberate signal so analytical questions have real answers
(not flat noise):
  - device, channel (referrer), country and user are fixed per session, so they
    can correlate with behavior the way they do in real traffic.
  - conversion (add_to_cart / purchase propensity) varies by segment: mobile
    converts worse than desktop, paid/social channels worse than search/direct,
    and a small pool of "power users" convert much better.
  - traffic follows a daily (diurnal) rhythm, with one anomalous spike day.
  - revenue is concentrated: power users and a few countries drive most of it.
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

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
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

# (value, share, conversion multiplier). Shares need not sum to exactly 1;
# random.choices normalizes them.
DEVICES = [
    ("desktop", 0.55, 1.0),
    ("mobile", 0.38, 0.45),
    ("tablet", 0.07, 0.8),
]
CHANNELS = [
    ("https://www.google.com/", 0.32, 1.30),
    ("", 0.28, 1.00),  # direct
    ("https://news.ycombinator.com/", 0.12, 1.15),
    ("https://www.bing.com/", 0.10, 0.90),
    ("https://twitter.com/", 0.18, 0.50),
]
# (country, share, average-order-value multiplier).
COUNTRIES = [
    ("US", 0.40, 1.20),
    ("GB", 0.12, 1.10),
    ("DE", 0.10, 1.10),
    ("IN", 0.08, 0.60),
    ("FR", 0.07, 1.00),
    ("BR", 0.06, 0.70),
    ("JP", 0.05, 1.40),
    ("CA", 0.05, 1.05),
    ("AU", 0.04, 1.00),
    ("NL", 0.03, 1.00),
]

# Relative traffic weight by hour of day (UTC). Overnight trough, midday/evening
# peak. Drives both when sessions start and, by extension, when purchases happen.
DIURNAL = [
    0.20, 0.15, 0.12, 0.10, 0.12, 0.20,  # 00-05
    0.40, 0.70, 1.00, 1.15, 1.25, 1.30,  # 06-11
    1.35, 1.30, 1.20, 1.20, 1.30, 1.45,  # 12-17
    1.55, 1.45, 1.15, 0.80, 0.50, 0.30,  # 18-23
]

# Power users: a small pool (Pareto tail) that converts and spends much more.
VIP_USER_MAX = 500
TOTAL_USERS = 50_000
VIP_SESSION_SHARE = 0.25
VIP_CONV_BOOST = 2.0
VIP_AOV_BOOST = 1.5

# One day in the middle of the backfill window gets a traffic + conversion spike
# (think flash sale) so anomaly-hunting questions have something to find.
SPIKE_EXTRA_SHARE = 0.14
SPIKE_CONV_BOOST = 1.6

# Seconds between consecutive events within the same session (backfill mode).
_MIN_EVENT_GAP_S = 5
_MAX_EVENT_GAP_S = 300

_event_names = [e for e, _ in EVENT_TYPES]
_PV_W, _CLICK_W, _CART_W, _PURCHASE_W = (w for _, w in EVENT_TYPES)

_device_vals = [d[0] for d in DEVICES]
_device_weights = [d[1] for d in DEVICES]
_channel_vals = [c[0] for c in CHANNELS]
_channel_weights = [c[1] for c in CHANNELS]
_country_vals = [c[0] for c in COUNTRIES]
_country_weights = [c[1] for c in COUNTRIES]
_device_conv = {d[0]: d[2] for d in DEVICES}
_channel_conv = {c[0]: c[2] for c in CHANNELS}
_country_aov = {c[0]: c[2] for c in COUNTRIES}
_hours = list(range(24))


class SessionPool:
    """Keeps a rolling set of active sessions so events look like real browsing
    sessions instead of fully random rows.

    Each session fixes its own user, device, channel, country and a conversion
    multiplier, and carries its own clock. In backfill mode (backfill_seconds > 0)
    a session starts at a diurnally-weighted time within the window (one day gets
    a deliberate spike) and its events advance the clock by small gaps, so a
    session's events stay clustered and ordered in time (which funnel / sequence /
    window queries rely on). In live mode events are stamped at the current time.
    """

    def __init__(self, size: int = 500, backfill_seconds: float = 0.0) -> None:
        self._size = size
        self._backfill_seconds = backfill_seconds
        self._days = max(1, int(round(backfill_seconds / 86400.0)))
        # Spike on a full day near the middle of the window (avoid partial edges).
        self._spike_day = max(1, min(self._days - 1, round(self._days * 0.5)))
        self._sessions: list[dict] = []

    def _start_time(self) -> tuple[datetime, bool]:
        """Pick a session start timestamp following the diurnal curve, biased
        toward the spike day. Returns (start, is_spike_day)."""
        now = datetime.now(timezone.utc)
        if self._backfill_seconds <= 0:
            return now, False

        is_spike = random.random() < SPIKE_EXTRA_SHARE
        day_offset = self._spike_day if is_spike else random.randint(0, self._days)
        hour = random.choices(_hours, weights=DIURNAL, k=1)[0]
        day = (now - timedelta(days=day_offset)).date()
        start = datetime(
            day.year, day.month, day.day,
            hour, random.randint(0, 59), random.randint(0, 59),
            random.randint(0, 999) * 1000,
            tzinfo=timezone.utc,
        )
        # Day 0 with a late hour can land in the future; pull it back.
        if start > now:
            start = now - timedelta(seconds=random.uniform(0, 3600))
        return start, is_spike

    def _new_session(self) -> dict:
        start, is_spike = self._start_time()

        is_vip = random.random() < VIP_SESSION_SHARE
        if is_vip:
            user_id = random.randint(1, VIP_USER_MAX)
        else:
            user_id = random.randint(VIP_USER_MAX + 1, TOTAL_USERS)

        device = random.choices(_device_vals, weights=_device_weights, k=1)[0]
        channel = random.choices(_channel_vals, weights=_channel_weights, k=1)[0]
        country = random.choices(_country_vals, weights=_country_weights, k=1)[0]

        conv = _device_conv[device] * _channel_conv[channel]
        if is_vip:
            conv *= VIP_CONV_BOOST
        if is_spike:
            conv *= SPIKE_CONV_BOOST

        aov = _country_aov[country]
        if is_vip:
            aov *= VIP_AOV_BOOST

        return {
            "user_id": user_id,
            "session_id": str(uuid.uuid4()),
            "clock": start,
            "device": device,
            "referrer": channel,
            "country": country,
            "browser": random.choice(BROWSERS),
            "conv": conv,
            "aov": aov,
        }

    def pick(self) -> tuple[dict, datetime]:
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
        return session, event_dt


def make_event(pool: SessionPool) -> dict:
    """Build one event. Segment attributes and event_time come from the session
    (see SessionPool); the session's conversion multiplier shapes how likely the
    event is to be an add_to_cart / purchase."""
    session, event_dt = pool.pick()

    conv = session["conv"]
    weights = [_PV_W, _CLICK_W, _CART_W * conv, _PURCHASE_W * conv]
    event_type = random.choices(_event_names, weights=weights, k=1)[0]

    price = 0.0
    if event_type in ("add_to_cart", "purchase"):
        price = round(random.uniform(9.99, 499.99) * session["aov"], 2)

    return {
        "event_id": str(uuid.uuid4()),
        # ISO-8601 with milliseconds; ClickHouse parses this into DateTime64(3).
        "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event_type": event_type,
        "user_id": session["user_id"],
        "session_id": session["session_id"],
        "url": random.choice(PATHS),
        "referrer": session["referrer"],
        "device": session["device"],
        "browser": session["browser"],
        "country": session["country"],
        "price": price,
    }
