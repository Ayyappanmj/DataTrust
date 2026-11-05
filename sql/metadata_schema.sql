-- DataTrust pipeline metadata schema (PostgreSQL). Applied automatically by the repository.
-- Timestamps are TIMESTAMPTZ; the SQLite variant stores ISO-8601 text.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    trigger_type      TEXT NOT NULL DEFAULT 'cli',
    status            TEXT NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    duration_seconds  DOUBLE PRECISION,
    input_files       TEXT,
    records_read      BIGINT NOT NULL DEFAULT 0,
    records_accepted  BIGINT NOT NULL DEFAULT 0,
    records_rejected  BIGINT NOT NULL DEFAULT 0,
    records_written   BIGINT NOT NULL DEFAULT 0,
    error_summary     TEXT,
    params            TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_manifest (
    checksum          TEXT PRIMARY KEY,
    file_path         TEXT NOT NULL,
    file_name         TEXT NOT NULL,
    file_format       TEXT NOT NULL,
    size_bytes        BIGINT NOT NULL,
    record_count      BIGINT,
    columns_json      TEXT,
    schema_fingerprint TEXT,
    ingest_status     TEXT NOT NULL DEFAULT 'PENDING',
    process_status    TEXT NOT NULL DEFAULT 'PENDING',
    active            INTEGER NOT NULL DEFAULT 0,
    first_seen_at     TIMESTAMPTZ NOT NULL,
    ingested_at       TIMESTAMPTZ,
    processed_at      TIMESTAMPTZ,
    last_run_id       TEXT,
    bronze_path       TEXT,
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_name      TEXT PRIMARY KEY,
    layer             TEXT NOT NULL,
    location          TEXT,
    partition_column  TEXT,
    description       TEXT,
    updated_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_results (
    id                BIGSERIAL PRIMARY KEY,
    run_id            TEXT NOT NULL,
    stage             TEXT NOT NULL,
    dataset_name      TEXT NOT NULL,
    rule_name         TEXT NOT NULL,
    rule_description  TEXT,
    severity          TEXT NOT NULL,
    action            TEXT,
    records_evaluated BIGINT NOT NULL,
    records_passed    BIGINT NOT NULL,
    records_failed    BIGINT NOT NULL,
    failure_pct       DOUBLE PRECISION NOT NULL,
    threshold_pct     DOUBLE PRECISION,
    status            TEXT NOT NULL,
    executed_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS processing_errors (
    id                BIGSERIAL PRIMARY KEY,
    run_id            TEXT NOT NULL,
    step              TEXT NOT NULL,
    error_type        TEXT NOT NULL,
    message           TEXT NOT NULL,
    details           TEXT,
    file_path         TEXT,
    occurred_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id                BIGSERIAL PRIMARY KEY,
    run_id            TEXT NOT NULL,
    source_dataset    TEXT NOT NULL,
    source_checksum   TEXT,
    transformation    TEXT NOT NULL,
    output_dataset    TEXT NOT NULL,
    output_record_count BIGINT,
    processed_at      TIMESTAMPTZ NOT NULL,
    status            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill_history (
    backfill_id       TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    start_date        TEXT NOT NULL,
    end_date          TEXT NOT NULL,
    partitions        TEXT,
    status            TEXT NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    records_written   BIGINT,
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS partition_metadata (
    dataset_name      TEXT NOT NULL,
    partition_value   TEXT NOT NULL,
    row_count         BIGINT NOT NULL,
    size_bytes        BIGINT NOT NULL,
    file_count        BIGINT NOT NULL,
    source_run_id     TEXT,
    updated_at        TIMESTAMPTZ NOT NULL,
    warehouse_loaded_at TIMESTAMPTZ,
    PRIMARY KEY (dataset_name, partition_value)
);

CREATE INDEX IF NOT EXISTS idx_quality_run ON quality_results (run_id);
CREATE INDEX IF NOT EXISTS idx_manifest_path ON ingestion_manifest (file_path, active);
CREATE INDEX IF NOT EXISTS idx_lineage_output ON lineage_edges (output_dataset);
CREATE INDEX IF NOT EXISTS idx_errors_run ON processing_errors (run_id);
