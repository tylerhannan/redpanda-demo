# AI + MCP demo prompts

Ask these in a chat connected to the ClickHouse MCP server (service: `redpanda-demo`).
They are written so the agent has to explore: pick its own tools, system tables and
cuts of the data, then interpret what it finds. None is a canned metric from
`03_showcase.sql`.

All were tested against the live dataset and return a real, non-obvious answer.

## How the data is shaped

The events are synthetic but carry deliberate signal, so business questions have
real answers instead of flat noise:

- `device`, channel (`referrer`), `country` and `user_id` are fixed per session, so
  they correlate with behavior the way real traffic does.
- conversion varies by segment: mobile converts about half as well as desktop, and
  paid/social traffic converts worse than search and direct.
- traffic follows a daily rhythm, and one day in the window has a deliberate spike.
- revenue is concentrated in a small pool of power users.

## Data and behavior

### 1. Where is conversion weakest?

> We want to prioritize one fix. Across devices, where is purchase conversion
> weakest, and how big is the gap versus the best device?

The agent chooses the cut, computes conversion per device, and quantifies a gap.

Expect: desktop around 7.6%, tablet around 6%, mobile around 3.4%. Mobile converts
roughly half as well as desktop despite heavy traffic.

### 2. Which acquisition channel earns its keep?

> If I could keep only a couple of acquisition channels, which ones actually
> convert? Rank referrers by purchase conversion.

Expect: Google and Hacker News convert best (around 7%), direct and Bing in the
middle, Twitter worst (around 3%). A clear spend-allocation story.

### 3. When do people actually buy?

> When is the best and worst time of day to run a flash sale, based on when people
> actually purchase?

Expect: purchases follow a daily curve, peaking late afternoon / early evening UTC
and bottoming out overnight (roughly 03:00-04:00 UTC).

Then, for an image:

> Turn that hourly purchase pattern into a line or area chart image.

### 4. Investigate the anomaly

> Something looks off in this week's traffic. Find the anomaly, and tell me whether
> it was just more visitors or also better conversion.

Open-ended investigation: the agent has to locate the outlier day, then decide
whether it was volume, conversion, or both.

Expect: one day carries roughly 2.3x the traffic of a normal day and also a higher
conversion rate (a planted flash-sale spike), not a data glitch.

Then, for an image:

> Turn the daily event counts into a bar chart image and highlight the spike day.

### 5. How concentrated is revenue?

> Is our revenue a broad base of customers or a few whales? Quantify how
> concentrated it is.

Expect: a small pool of power users (about 1% of users) drives close to half of all
purchase revenue. A textbook Pareto tail.

Then, for an image:

> Turn that into a chart image: cumulative share of revenue by user percentile.

## Infrastructure and storage

### 6. Is the pipeline healthy?

> Check my ClickPipes setup on this service. Is the Redpanda pipe running, what
> topic and auth is it using, and is ClickHouse keeping up with the stream right now?

Reads ClickPipe state through the management API (not SQL), then runs a freshness
query comparing `max(event_time)` to `now()`.

Expect: state `Running`, topic `clickstream_events`, `SCRAM-SHA-256`, managed table,
plus the current ingestion lag in seconds.

### 7. Which columns compress, and which don't?

> For clickstream_events, which columns take the most and least space on disk?
> Explain why some compress far better than others.

Expect: `event_type` and `country` compress roughly 1000x (LowCardinality, very few
distinct values), `price` around 30x, and `event_id` (a random UUID) about 1x because
there is no redundancy to remove. The random UUID columns dominate the footprint.

Then, for an image:

> Turn the per-column compression ratios into a bar chart image on a log scale.

### 8. Audit the table layout

> Audit clickstream_events for a healthy MergeTree layout: partitions, number of
> active parts, and anything that could slow ingestion or queries as it grows.

Expect: a single monthly partition, a handful of active parts, well merged. Monthly
partitioning is fine at this volume.

### 9. What has been hitting the service?

> What queries have actually run against this service in the last two hours? How
> many, what kinds, and how much data did they scan?

Self-observability through `system.query_log`. Expect thousands of SELECTs averaging
about 20ms, slower INSERTs (the ClickPipes batches), and the DDL from setup.

### Storage talking point: Redpanda vs ClickHouse

The same events live in both systems in different shapes. The agent can measure the
ClickHouse side over MCP (`system.parts` for `bytes_on_disk`); the Redpanda topic
size comes from the Redpanda console or `rpk`, since no MCP tool exposes it. Roughly:
each event is about 318 bytes as raw JSON, the Redpanda topic keeps a row log
replicated 3x, and ClickHouse stores the columnar copy at roughly 40 bytes per row.
The point is the roles: Redpanda is the durable buffer, ClickHouse the compact
long-term store.

---

The agent chooses tools and SQL on its own. If a data query returns
`Service does not allow MCP calls`, enable the remote MCP server on the service
(Connect, then MCP, then toggle on).
