"""Shared fixtures. Spark/DuckDB/dbt fixtures skip cleanly when the library is missing."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import Settings  # noqa: E402
from src.metadata.repository import SqliteRepository  # noqa: E402
from src.sample.generator import generate_samples  # noqa: E402


@pytest.fixture
def settings(tmp_path) -> Settings:
    cfg = Settings(data_root=tmp_path / "data", shuffle_partitions=2, spark_master="local[2]",
                   spark_driver_memory="1g")
    cfg.ensure_dirs()
    return cfg


@pytest.fixture
def repo(settings):
    repository = SqliteRepository(settings.sqlite_path)
    repository.init_schema()
    yield repository
    repository.close()


@pytest.fixture
def sample_dir(tmp_path):
    """Two small deterministic months (Jan, Feb) written as CSV."""
    out = tmp_path / "input"
    generate_samples(out, ["2023-01", "2023-02"], valid_rows=300, per_injection=4, malformed_rows=0)
    return out


@pytest.fixture(scope="session")
def spark_session():
    pytest.importorskip("pyspark")
    from src.processing.spark_session import get_spark

    cfg = Settings(shuffle_partitions=2, spark_master="local[2]", spark_driver_memory="1g")
    spark = get_spark(cfg, "datatrust-tests")
    yield spark
    spark.stop()


@pytest.fixture
def pg_settings(settings):
    """PostgreSQL settings from DATATRUST_TEST_PG_* variables; skips when not provided."""
    host = os.environ.get("DATATRUST_TEST_PG_HOST")
    if not host:
        pytest.skip("set DATATRUST_TEST_PG_HOST/PORT/DB/USER/PASSWORD to run PostgreSQL tests")
    return settings.with_overrides(
        metadata_backend="postgres", pg_host=host,
        pg_port=int(os.environ.get("DATATRUST_TEST_PG_PORT", "5432")),
        pg_db=os.environ.get("DATATRUST_TEST_PG_DB", "datatrust"),
        pg_user=os.environ.get("DATATRUST_TEST_PG_USER", "datatrust"),
        pg_password=os.environ.get("DATATRUST_TEST_PG_PASSWORD", ""))
