"""Bronze ingestion: write one immutable Parquet folder per source-file checksum."""
from __future__ import annotations

import math
import shutil
from typing import Any

from src.common.config import Settings
from src.common.logging_config import get_logger
from src.ingestion.csv_reader import read_csv
from src.ingestion.file_discovery import DiscoveredFile, FileValidation, short_checksum
from src.ingestion.parquet_reader import read_parquet_as_strings
from src.storage.parquet_storage import uri

log = get_logger(__name__)
TARGET_FILE_BYTES = 128 * 1024 * 1024


def bronze_path(settings: Settings, checksum: str):
    return settings.bronze_dir / f"source_checksum={short_checksum(checksum)}"


def ingest_file(spark: Any, settings: Settings, file: DiscoveredFile,
                validation: FileValidation, run_id: str) -> int:
    """Write the file to Bronze (atomically) and return the number of records ingested.

    The original input file is never modified or moved. Bronze rows keep the original column
    names and raw string values plus source metadata columns.
    """
    from pyspark.sql import functions as F

    if file.file_format == "csv":
        df = read_csv(spark, file.path, validation.columns, settings.csv_delimiter)
    else:
        df = read_parquet_as_strings(spark, file.path)
    df = (
        df.withColumn("_source_file", F.lit(file.path.name))
        .withColumn("_source_checksum", F.lit(short_checksum(file.checksum)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_row_id", F.monotonically_increasing_id())
    )
    # a few well-sized files instead of many tiny ones
    n_files = max(1, math.ceil(file.size_bytes / TARGET_FILE_BYTES))
    staged = settings.staging_dir / run_id / "bronze" / f"source_checksum={short_checksum(file.checksum)}"
    if staged.exists():
        shutil.rmtree(staged)
    df.repartition(n_files).write.mode("overwrite").parquet(uri(staged))
    count = spark.read.parquet(uri(staged)).count()

    target = bronze_path(settings, file.checksum)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.move(str(staged), str(target))
    log.info("Bronze written", extra={"file": file.path.name, "records": count, "run_id": run_id})
    return count
