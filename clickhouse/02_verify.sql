-- Run these after the ClickPipe is live and the producer is sending data.

-- 1. Is data landing? Watch this number climb while the producer runs.
SELECT count() AS rows, max(event_time) AS latest_event
FROM default.clickstream_events;

-- 2. Most recent events.
SELECT event_time, event_type, user_id, country, url, price
FROM default.clickstream_events
ORDER BY event_time DESC
LIMIT 20;

-- 3. Event mix (uses the ORDER BY prefix -> fast).
SELECT event_type, count() AS events
FROM default.clickstream_events
GROUP BY event_type
ORDER BY events DESC;

-- 4. Live-ish revenue by country from purchases.
SELECT country, round(sum(price), 2) AS revenue, count() AS purchases
FROM default.clickstream_events
WHERE event_type = 'purchase'
GROUP BY country
ORDER BY revenue DESC;

-- 5. Events per minute over the last hour (time-series view).
SELECT toStartOfMinute(event_time) AS minute, count() AS events
FROM default.clickstream_events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;

-- 6. Simple funnel: sessions that viewed -> added to cart -> purchased.
SELECT
    countDistinctIf(session_id, event_type = 'page_view')   AS viewed,
    countDistinctIf(session_id, event_type = 'add_to_cart') AS carted,
    countDistinctIf(session_id, event_type = 'purchase')    AS purchased
FROM default.clickstream_events;
