"""Read a Parquet source file and present it in the same all-string Bronze shape as CSV."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.storage.parquet_storage import uri


def read_parquet_as_strings(spark: Any, path: Path) -> Any:
    """Cast every column to string; timestamps are rendered as 'yyyy-MM-dd HH:mm:ss'."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, TimestampType

    df = spark.read.parquet(uri(path))
    projected = []
    for field in df.schema.fields:
        col = F.col(f"`{field.name}`")
        if isinstance(field.dataType, TimestampType):
            projected.append(F.date_format(col, "yyyy-MM-dd HH:mm:ss").alias(field.name))
        elif isinstance(field.dataType, DateType):
            projected.append(F.date_format(col, "yyyy-MM-dd").alias(field.name))
        else:
            projected.append(col.cast("string").alias(field.name))
    return df.select(*projected).withColumn("_corrupt_record", F.lit(None).cast("string"))
