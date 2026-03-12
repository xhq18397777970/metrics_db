CREATE MATERIALIZED VIEW IF NOT EXISTS metric_rollup_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 hour', bucket_time) AS bucket_time,
    cluster_name,
    metric_name,
    labels,
    labels_fingerprint,
    COUNT(*)::BIGINT AS sample_count,
    AVG(metric_value) AS avg_value,
    MIN(metric_value) AS min_value,
    MAX(metric_value) AS max_value
FROM metric_points
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

ALTER MATERIALIZED VIEW metric_rollup_1h
    SET (timescaledb.materialized_only = true);

CREATE INDEX IF NOT EXISTS idx_metric_rollup_1h_cluster_metric_bucket
    ON metric_rollup_1h (cluster_name, metric_name, bucket_time DESC);

SELECT add_continuous_aggregate_policy(
    'metric_rollup_1h',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    initial_start => TIMESTAMPTZ '2100-01-01 00:00:00+00',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS metric_rollup_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', bucket_time) AS bucket_time,
    cluster_name,
    metric_name,
    labels,
    labels_fingerprint,
    SUM(sample_count)::BIGINT AS sample_count,
    SUM(avg_value * sample_count) / NULLIF(SUM(sample_count), 0) AS avg_value,
    MIN(min_value) AS min_value,
    MAX(max_value) AS max_value
FROM metric_rollup_1h
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

ALTER MATERIALIZED VIEW metric_rollup_1d
    SET (timescaledb.materialized_only = true);

CREATE INDEX IF NOT EXISTS idx_metric_rollup_1d_cluster_metric_bucket
    ON metric_rollup_1d (cluster_name, metric_name, bucket_time DESC);

SELECT add_continuous_aggregate_policy(
    'metric_rollup_1d',
    start_offset => INTERVAL '180 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    initial_start => TIMESTAMPTZ '2100-01-01 00:00:00+00',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'metric_points',
    drop_after => INTERVAL '180 days',
    schedule_interval => INTERVAL '1 day',
    initial_start => TIMESTAMPTZ '2100-01-01 00:00:00+00',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'metric_rollup_1h',
    drop_after => INTERVAL '365 days',
    schedule_interval => INTERVAL '1 day',
    initial_start => TIMESTAMPTZ '2100-01-01 00:00:00+00',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'metric_rollup_1d',
    drop_after => INTERVAL '730 days',
    schedule_interval => INTERVAL '1 day',
    initial_start => TIMESTAMPTZ '2100-01-01 00:00:00+00',
    if_not_exists => TRUE
);
