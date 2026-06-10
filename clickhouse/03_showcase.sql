-- ===========================================================================
-- ClickHouse feature tour: run against default.clickstream_events
-- ===========================================================================
-- Each section highlights a ClickHouse strength:
-- real-time analytics, purpose-built funnel/retention functions, approximate
-- algorithms, window functions, and incremental materialized views.
-- All queries are read-only except the clearly marked materialized-view section.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 0. Live ingestion check: re-run while the producer + ClickPipes stream new
--    events to watch the row count climb. seconds_behind shows ingestion lag.
-- ---------------------------------------------------------------------------
SELECT
    formatReadableQuantity(count())     AS total_rows,
    max(event_time)                     AS latest_event,
    dateDiff('second', max(event_time), now()) AS seconds_behind
FROM default.clickstream_events;


-- ---------------------------------------------------------------------------
-- 1. Raw scan speed: full-table aggregation in milliseconds.
--    countIf/sumIf let you compute many conditional metrics in ONE pass.
--    uniq() is the approximate distinct count (HyperLogLog): it uses a fraction
--    of the memory of uniqExact and is dramatically faster on high-cardinality
--    columns like session_id, with typical error well under 1%. Section 5 below
--    contrasts the two directly.
-- ---------------------------------------------------------------------------
SELECT
    count()                                   AS total_events,
    uniq(user_id)                             AS unique_users,
    uniq(session_id)                          AS sessions,
    countIf(event_type = 'purchase')          AS purchases,
    round(sumIf(price, event_type = 'purchase'), 2) AS revenue,
    round(avgIf(price, event_type = 'purchase'), 2) AS avg_order_value
FROM default.clickstream_events;


-- ---------------------------------------------------------------------------
-- 2. Conversion funnel with windowFunnel(): page_view -> add_to_cart ->
--    purchase, all within a 1-hour window, in event order. This is a single
--    function that would otherwise require multiple self-joins.
-- ---------------------------------------------------------------------------
SELECT
    level,
    count()                                     AS sessions,
    round(100 * count() / sum(count()) OVER (), 1) AS pct_of_sessions
FROM
(
    SELECT
        session_id,
        windowFunnel(3600)(
            toDateTime(event_time),
            event_type = 'page_view',
            event_type = 'add_to_cart',
            event_type = 'purchase'
        ) AS level
    FROM default.clickstream_events
    GROUP BY session_id
)
GROUP BY level
ORDER BY level;
-- level 0 = no page_view, 1 = viewed, 2 = +add_to_cart, 3 = full purchase path.


-- ---------------------------------------------------------------------------
-- 3. Ordered pattern matching with sequenceCount(): how often did a session
--    view a page and *later* purchase?
-- ---------------------------------------------------------------------------
SELECT
    sequenceCount('(?1).*(?2)')(
        toDateTime(event_time),
        event_type = 'page_view',
        event_type = 'purchase'
    ) AS view_then_purchase_sequences
FROM default.clickstream_events;


-- ---------------------------------------------------------------------------
-- 4. N-day retention without dimension tables: for each user, how many days
--    after their first visit do we still see them? Self-contained cohort.
-- ---------------------------------------------------------------------------
WITH user_days AS
(
    SELECT
        user_id,
        min(toDate(event_time))            AS first_day,
        groupUniqArray(toDate(event_time)) AS active_days
    FROM default.clickstream_events
    GROUP BY user_id
)
SELECT
    days_since_first,
    count() AS users
FROM user_days
ARRAY JOIN arrayMap(d -> toUInt32(dateDiff('day', first_day, d)), active_days) AS days_since_first
GROUP BY days_since_first
ORDER BY days_since_first;


-- ---------------------------------------------------------------------------
-- 5. Approximate analytics: exact vs probabilistic distinct counts.
--    uniq()/uniqHLL12() use tiny memory and are near-instant at scale, with
--    typical error < 1%. Great for dashboards over billions of rows.
--    user_id is low cardinality (<=50k), so all three agree exactly. session_id
--    is high cardinality (hundreds of thousands), so the approximate functions
--    diverge slightly from uniqExact while using far less memory.
-- ---------------------------------------------------------------------------
SELECT
    uniqExact(user_id)    AS users_exact,
    uniq(user_id)         AS users_approx,
    uniqExact(session_id) AS sessions_exact,
    uniq(session_id)      AS sessions_approx,
    uniqHLL12(session_id) AS sessions_hll
FROM default.clickstream_events;


-- ---------------------------------------------------------------------------
-- 6. topK(): approximate most-frequent values in a single pass.
-- ---------------------------------------------------------------------------
SELECT
    topK(5)(url)      AS top_pages,
    topK(5)(referrer) AS top_referrers,
    topK(3)(country)  AS top_countries
FROM default.clickstream_events;


-- ---------------------------------------------------------------------------
-- 7. Price distribution with quantiles (t-digest): p50/p90/p99 of order value
--    per event type, computed in one streaming pass.
-- ---------------------------------------------------------------------------
SELECT
    event_type,
    count()                                                AS n,
    round(avg(price), 2)                                   AS avg_price,
    arrayMap(x -> round(x, 2),
        quantilesTDigest(0.5, 0.9, 0.99)(toFloat64(price))) AS p50_p90_p99
FROM default.clickstream_events
WHERE price > 0
GROUP BY event_type
ORDER BY n DESC;


-- ---------------------------------------------------------------------------
-- 8. Window functions: sessionize a single session and measure the gap
--    between consecutive events with lagInFrame().
-- ---------------------------------------------------------------------------
SELECT
    event_time,
    event_type,
    url,
    row_number() OVER w AS step,
    -- NULL on the first event of the session (no previous row to diff against).
    if(
        row_number() OVER w = 1,
        NULL,
        dateDiff('second', lagInFrame(toDateTime(event_time)) OVER w, toDateTime(event_time))
    ) AS secs_since_prev
FROM default.clickstream_events
WHERE session_id =
(
    -- pick the busiest session as an example
    SELECT session_id
    FROM default.clickstream_events
    GROUP BY session_id
    ORDER BY count() DESC
    LIMIT 1
)
WINDOW w AS (PARTITION BY session_id ORDER BY event_time)
ORDER BY event_time;


-- ---------------------------------------------------------------------------
-- 9. argMax(): latest known page per user without a correlated subquery.
-- ---------------------------------------------------------------------------
SELECT
    user_id,
    argMax(url, event_time)        AS last_page,
    argMax(device, event_time)     AS last_device,
    max(event_time)                AS last_seen,
    count()                        AS events
FROM default.clickstream_events
GROUP BY user_id
ORDER BY last_seen DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 10. Gap-free time series with WITH FILL: events per minute over the last
--     hour, including minutes that had zero events.
-- ---------------------------------------------------------------------------
SELECT
    toStartOfMinute(event_time) AS minute,
    count()                     AS events,
    countIf(event_type = 'purchase') AS purchases
FROM default.clickstream_events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute WITH FILL STEP INTERVAL 1 MINUTE;


-- ---------------------------------------------------------------------------
-- 11. Multi-level subtotals with ROLLUP: events by country, then by
--     country+device, plus a grand total, in one query.
-- ---------------------------------------------------------------------------
SELECT
    country,
    device,
    count() AS events
FROM default.clickstream_events
GROUP BY ROLLUP(country, device)
ORDER BY country, device;


-- ===========================================================================
-- 12. (OPTIONAL) Real-time rollup with an incremental Materialized View.
--     The MV runs on each insert block, so the summary table is always current
--     and queries hit pre-aggregated data. This is the ClickHouse pattern for
--     real-time dashboards. Run once to set up, then query mv_events_per_minute.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS default.events_per_minute
(
    minute      DateTime,
    event_type  LowCardinality(String),
    events      UInt64,
    revenue     Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY (minute, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_events_per_minute
TO default.events_per_minute
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    event_type,
    count()    AS events,
    sum(price) AS revenue
FROM default.clickstream_events
GROUP BY minute, event_type;

-- Note: the MV only sees rows inserted AFTER it is created. To include existing
-- rows once, backfill:
--   INSERT INTO default.events_per_minute
--   SELECT toStartOfMinute(event_time), event_type, count(), sum(price)
--   FROM default.clickstream_events GROUP BY 1, 2;

-- Query the TARGET table (events_per_minute), NOT the view (mv_events_per_minute).
-- With the "TO <table>" syntax the materialized view stores no data of its own;
-- it is just an insert trigger that writes rollups into events_per_minute. That
-- target table is where the data physically lives, so it is what you query.
-- (Only MVs created WITHOUT "TO" keep their own .inner storage and are queried
-- by the view name.)
--
-- We GROUP BY on read instead of using FINAL because SummingMergeTree merges
-- partial sums in the background; summing at query time is correct and cheap.
SELECT minute, event_type, sum(events) AS events, sum(revenue) AS revenue
FROM default.events_per_minute
GROUP BY minute, event_type
ORDER BY minute DESC, events DESC
LIMIT 20;
