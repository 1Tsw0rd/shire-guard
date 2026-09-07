CREATE TABLE IF NOT EXISTS shire.security_events (
    event_type String,
    source_ip String,
    failed_count UInt32,
    success_count UInt32,
    username String,
    timestamp DateTime64(3)
) ENGINE = MergeTree()
ORDER BY timestamp;