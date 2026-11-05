"""Spark session factory with settings that matter for correctness and for Windows."""
from __future__ import annotations

import os
import sys
from typing import Any

from src.common.config import Settings


def get_spark(settings: Settings, app_name: str = "datatrust") -> Any:
    """Create (or reuse) a local SparkSession.

    Notes
    * Session time zone is UTC and TLC timestamps are treated as naive wall-clock values: they
      are parsed and stored without any shifting.
    * ANSI mode is off so invalid casts become NULL; the quality rules then report them.
    * Partition column type inference is off so ``pickup_date``/``pickup_month`` stay strings.
    * RawLocalFileSystem avoids .crc side files and the native Windows checksum filesystem.
    """
    from pyspark.sql import SparkSession

    from pathlib import Path
    py_dir = str(Path(sys.executable).parent)
    if py_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{py_dir};{os.environ.get('PATH', '')}"

    if " " in sys.executable:
        os.environ.setdefault("PYSPARK_PYTHON", "python")
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")
    else:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Automatically detect embedded portable Java and Hadoop if not already set
    from src.common.config import PROJECT_ROOT
    if "JAVA_HOME" not in os.environ:
        for candidate in (PROJECT_ROOT / ".jdk").glob("jdk-*"):
            if candidate.is_dir() and (candidate / "bin" / "java.exe").is_file():
                os.environ["JAVA_HOME"] = str(candidate)
                os.environ["PATH"] = f"{candidate / 'bin'};{os.environ.get('PATH', '')}"
                break
    if "HADOOP_HOME" not in os.environ:
        hadoop_dir = PROJECT_ROOT / ".hadoop"
        if (hadoop_dir / "bin" / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = str(hadoop_dir)
            os.environ["PATH"] = f"{hadoop_dir / 'bin'};{os.environ.get('PATH', '')}"

    builder = (
        SparkSession.builder.master(settings.spark_master)
        .appName(app_name)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
