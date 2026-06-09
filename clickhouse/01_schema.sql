-- Destination table for the Redpanda -> ClickPipes demo.
-- Run this in your ClickHouse Cloud SQL console BEFORE creating the ClickPipe,
-- then point the ClickPipe at this existing table.
--
-- Design notes (ClickHouse best practices):
--   * ORDER BY goes low -> high cardinality and leads with the columns we
--     filter/group on most: event_type (~4), country (~10), event_time (high).
--   * LowCardinality(String) for columns with well under 10K distinct values.
--   * Native types instead of String-for-everything (UInt64, DateTime64, Decimal).
--   * No Nullable columns; DEFAULTs are used instead.
--   * PARTITION BY month keeps the partition count small and enables cheap
--     data lifecycle management (TTL / DROP PARTITION) later.

CREATE TABLE IF NOT EXISTS default.clickstream_events
(
    event_id    UUID,
    event_time  DateTime64(3),
    event_type  LowCardinality(String),
    user_id     UInt64,
    session_id  UUID,
    url         String,
    referrer    String           DEFAULT '',
    device      LowCardinality(String),
    browser     LowCardinality(String),
    country     LowCardinality(String),
    price       Decimal(10, 2)   DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, country, event_time)
-- Keep raw events for 90 days; drop older partitions automatically.
TTL toDateTime(event_time) + INTERVAL 90 DAY;
