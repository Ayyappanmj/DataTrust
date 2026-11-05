"""Read a CSV into a Spark DataFrame with an explicit all-string schema.

Bronze keeps raw text on purpose: casting happens in Silver, where failures can be counted,
explained and quarantined instead of silently becoming NULL at ingestion time.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.storage.parquet_storage import uri


def read_csv(spark: Any, path: Path, columns: list[str], delimiter: str = ",") -> Any:
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [StructField(c, StringType(), True) for c in columns]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("sep", delimiter)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding", "UTF-8")
        .option("multiLine", False)
        .csv(uri(path))
    )
