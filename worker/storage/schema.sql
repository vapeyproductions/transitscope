CREATE TABLE IF NOT EXISTS publish_versions (
    version BIGINT PRIMARY KEY,
    status VARCHAR NOT NULL CHECK (status IN ('building', 'published')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    published_at TIMESTAMP WITH TIME ZONE,
    note VARCHAR
);

CREATE TABLE IF NOT EXISTS target_manifest (
    target_id VARCHAR PRIMARY KEY,
    provider_target_id VARCHAR NOT NULL,
    ra_deg DOUBLE,
    dec_deg DOUBLE,
    sector INTEGER,
    cadence VARCHAR,
    source_product_family VARCHAR,
    selection_bucket VARCHAR,
    manifest_status VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id VARCHAR PRIMARY KEY,
    provider VARCHAR NOT NULL,
    query_config_json VARCHAR NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR NOT NULL,
    targets_seen INTEGER NOT NULL DEFAULT 0,
    products_downloaded INTEGER NOT NULL DEFAULT 0,
    products_failed INTEGER NOT NULL DEFAULT 0,
    error_json VARCHAR
);

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id VARCHAR PRIMARY KEY,
    target_id VARCHAR NOT NULL,
    current_stage VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    overall_score DOUBLE,
    stage_score DOUBLE,
    rank_overall INTEGER,
    rank_stage INTEGER,
    activity VARCHAR NOT NULL,
    rank_delta INTEGER,
    movement_kind VARCHAR,
    last_evaluated_at TIMESTAMP WITH TIME ZONE,
    known_match_id VARCHAR,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS evaluations (
    evaluation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    stage VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL,
    data_window_start TIMESTAMP WITH TIME ZONE,
    data_window_end TIMESTAMP WITH TIME ZONE,
    input_product_ids VARCHAR NOT NULL,
    feature_json VARCHAR NOT NULL,
    metric_json VARCHAR NOT NULL,
    classification VARCHAR,
    data_sufficiency VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS pipeline_events (
    event_id VARCHAR PRIMARY KEY,
    sequence_number BIGINT NOT NULL,
    candidate_id VARCHAR,
    stage VARCHAR,
    event_type VARCHAR NOT NULL,
    payload_json VARCHAR NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    model_run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS ranking_snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    view VARCHAR NOT NULL,
    sequence_number BIGINT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    candidate_order_json VARCHAR NOT NULL,
    active_candidate_count INTEGER NOT NULL DEFAULT 0,
    processed_target_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (view, sequence_number)
);

CREATE TABLE IF NOT EXISTS batch_runs (
    batch_id VARCHAR PRIMARY KEY,
    stage VARCHAR NOT NULL,
    selection_policy VARCHAR NOT NULL,
    requested_count INTEGER NOT NULL,
    selected_count INTEGER NOT NULL,
    evaluated_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    max_workers INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    error_json VARCHAR
);

CREATE TABLE IF NOT EXISTS processing_checkpoints (
    stage VARCHAR PRIMARY KEY,
    manifest_path VARCHAR NOT NULL,
    cursor BIGINT NOT NULL,
    universe_count BIGINT NOT NULL,
    processed_count BIGINT NOT NULL,
    last_batch_id VARCHAR,
    selection_policy VARCHAR NOT NULL,
    adaptive_sampling_enabled BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

