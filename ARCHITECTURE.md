# Architecture

## Layers (beginner-friendly)

- **Bronze = the raw copy.** Each source file is stored exactly as read (all values as text) in
  `data/bronze/trips/source_checksum=<id>/`, plus metadata columns (`_source_file`, `_source_checksum`,
  `_ingested_at`, `_source_row_id`). Nothing is cleaned or dropped here, and the original input file is never modified.
- **Silver = the clean table.** PySpark renames columns, casts types, parses timestamps, evaluates quality rules, removes
  duplicates, and writes valid rows to `data/silver/trips/pickup_date=YYYY-MM-DD/`. Invalid rows are not lost: they go to
  `data/quarantine/trips/` with `rejection_reason` and the raw record as JSON.
- **Gold = the answers.** DuckDB/dbt models turn Silver into small tables for questions (daily volume, revenue, hourly
  patterns, locations, payment types, quality summary, run summary). They live in the DuckDB file and are exported to `data/gold/*.parquet`.

## Idempotency

Running the same thing twice must not change the result. DataTrust gets this from: (1) checksums - an unchanged file is skipped;
(2) *replace, don't append* - a file's rows are removed from Silver before its new rows are added; (3) a record id
(SHA-256 of the business key) used to drop duplicates; (4) warehouse loading as `DELETE partition` + `INSERT` in one transaction;
(5) models rebuilt with `DROP/CREATE`.

## Incremental processing

File-level: the manifest decides per file (see README). Silver is rewritten **per partition**: affected partitions = those that
contain new rows or rows of a replaced file. Existing rows of the other files in those partitions are read back, merged and
written again. Cross-file duplicates are found by anti-joining against existing rows in the same partitions.

## Safe writes and recovery

1. Spark writes the new Silver/Quarantine partitions to `data/_staging/<run_id>/`.
2. The quality rules run on that result; a blocking failure stops the run - Silver is untouched.
3. `StagingArea.commit` writes a journal (`COMMITTING`, with the per-partition action), swaps only the affected
   partitions (old ones go to `_trash` first) and marks the journal `COMMITTED`.
4. At the start of every run, `recover_incomplete_commits` rolls any `COMMITTING` journal forward (the swap is idempotent).
5. The manifest marks a file `process_status=SUCCEEDED` only **after** the commit; metadata updates after a crash are redone by a RETRY.
6. Warehouse loading is checkpointed per partition (`partition_metadata.warehouse_loaded_at`), so a failed load resumes where it stopped.

| Failure | Automatically recoverable? |
|---|---|
| Corrupted/empty file, missing required column | Recorded, file FAILED, others continue. **Manual**: fix/replace the file |
| Unexpected columns | Policy `warn` (default) continues; `fail` marks the file FAILED |
| Spark crash / killed process before commit | Yes - rerun (manifest still PENDING) |
| Crash during commit swap | Yes - journal roll-forward on next run |
| Quality gate failure | Data stays staged/quarantined; **manual**: inspect report, adjust rules or data |
| DuckDB load failure | Yes - rerun loads pending partitions |
| dbt/model failure | Yes - rerun rebuilds models |
| Stale `RUNNING` run | Marked FAILED by the next run; stale lock file removable with `unlock` |
| Disk full / corrupted Parquet in Silver | **Manual** (restore from Bronze with a backfill) |

## Partitioning strategy

Daily `pickup_date` by default: queries and backfills are date-oriented, a month of TLC data gives about 30 partitions of
~100k rows, so there are few small files (`repartition` by the partition column writes one file per partition per write).
`monthly` is available for small datasets. Partition values are validated (`[A-Za-z0-9_.-]`) so a value can never escape the data directory.

## Metadata and lineage

Repository with two explicit backends (SQLite default, PostgreSQL). Lineage edges: `file:<path> -> bronze.trips` (checksum),
`bronze.trips -> silver.trips` (checksum), `silver.trips -> gold.<table>`. `lineage_upstream` walks edges with a recursive CTE and ignores
superseded file versions. **Limits:** dataset-level only; gold edges do not say which rows came from which file; no column lineage.

## Run orchestration

The same step functions power `python -m src.cli run` and the Airflow DAG (`step` command). Cross-task state is a small JSON
file per run (`data/_state`), the manifest, the staging plan and the metadata store.
