-- DataTrust pipeline metadata schema (SQLite, lightweight mode).
-- Same tables and columns as metadata_schema.sql; timestamps are ISO-8601 text.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    trigger_type      TEXT NOT NULL DEFAULT 'cli',
    status            TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    ended_at          TEXT,
    duration_seconds  REAL,
    input_files       TEXT,
    records_read      INTEGER NOT NULL DEFAULT 0,
    records_accepted  INTEGER NOT NULL DEFAULT 0,
    records_rejected  INTEGER NOT NULL DEFAULT 0,
    records_written   INTEGER NOT NULL DEFAULT 0,
    error_summary     TEXT,
    params            TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_manifest (
    checksum          TEXT PRIMARY KEY,
    file_path         TEXT NOT NULL,
    file_name         TEXT NOT NULL,
    file_format       TEXT NOT NULL,
    size_bytes        INTEGER NOT NULL,
    record_count      INTEGER,
    columns_json      TEXT,
    schema_fingerprint TEXT,
    ingest_status     TEXT NOT NULL DEFAULT 'PENDING',
    process_status    TEXT NOT NULL DEFAULT 'PENDING',
    active            INTEGER NOT NULL DEFAULT 0,
    first_seen_at     TEXT NOT NULL,
    ingested_at       TEXT,
    processed_at      TEXT,
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
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_results (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    stage             TEXT NOT NULL,
    dataset_name      TEXT NOT NULL,
    rule_name         TEXT NOT NULL,
    rule_description  TEXT,
    severity          TEXT NOT NULL,
    action            TEXT,
    records_evaluated INTEGER NOT NULL,
    records_passed    INTEGER NOT NULL,
    records_failed    INTEGER NOT NULL,
    failure_pct       REAL NOT NULL,
    threshold_pct     REAL,
    status            TEXT NOT NULL,
    executed_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processing_errors (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    step              TEXT NOT NULL,
    error_type        TEXT NOT NULL,
    message           TEXT NOT NULL,
    details           TEXT,
    file_path         TEXT,
    occurred_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    source_dataset    TEXT NOT NULL,
    source_checksum   TEXT,
    transformation    TEXT NOT NULL,
    output_dataset    TEXT NOT NULL,
    output_record_count INTEGER,
    processed_at      TEXT NOT NULL,
    status            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill_history (
    backfill_id       TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    start_date        TEXT NOT NULL,
    end_date          TEXT NOT NULL,
    partitions        TEXT,
    status            TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    ended_at          TEXT,
    records_written   INTEGER,
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS partition_metadata (
    dataset_name      TEXT NOT NULL,
    partition_value   TEXT NOT NULL,
    row_count         INTEGER NOT NULL,
    size_bytes        INTEGER NOT NULL,
    file_count        INTEGER NOT NULL,
    source_run_id     TEXT,
    updated_at        TEXT NOT NULL,
    warehouse_loaded_at TEXT,
    PRIMARY KEY (dataset_name, partition_value)
);

CREATE INDEX IF NOT EXISTS idx_quality_run ON quality_results (run_id);
CREATE INDEX IF NOT EXISTS idx_manifest_path ON ingestion_manifest (file_path, active);
CREATE INDEX IF NOT EXISTS idx_lineage_output ON lineage_edges (output_dataset);
CREATE INDEX IF NOT EXISTS idx_errors_run ON processing_errors (run_id);
