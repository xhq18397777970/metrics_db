CREATE TABLE IF NOT EXISTS metric_points (
    bucket_time         TIMESTAMPTZ NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    cluster_name        TEXT NOT NULL,
    metric_name         TEXT NOT NULL,
    metric_value        DOUBLE PRECISION NOT NULL,
    labels              JSONB NOT NULL DEFAULT '{}'::jsonb,
    labels_fingerprint  TEXT NOT NULL,
    source_tool         TEXT NOT NULL,
    collected_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket_time, cluster_name, metric_name, labels_fingerprint)
);

SELECT create_hypertable(
    'metric_points',
    by_range('bucket_time', INTERVAL '1 day'),
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_metric_points_cluster_metric_bucket
    ON metric_points (cluster_name, metric_name, bucket_time DESC);

CREATE TABLE IF NOT EXISTS collection_runs (
    run_id            UUID PRIMARY KEY,
    bucket_time       TIMESTAMPTZ NOT NULL,
    cluster_name      TEXT NOT NULL,
    collector_name    TEXT NOT NULL,
    status            TEXT NOT NULL,
    retry_count       INTEGER NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL,
    error_code        TEXT NULL,
    error_message     TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_collection_runs_bucket_status
    ON collection_runs (bucket_time, status);

CREATE INDEX IF NOT EXISTS idx_collection_runs_cluster_bucket
    ON collection_runs (cluster_name, bucket_time DESC);

CREATE INDEX IF NOT EXISTS idx_collection_runs_collector_bucket
    ON collection_runs (collector_name, bucket_time DESC);
