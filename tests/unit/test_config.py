import pytest

from src.common.config import Settings, safe_join, validate_safe_value
from src.common.exceptions import ConfigError


def test_defaults_are_valid_and_lightweight():
    s = Settings.from_env({})
    assert s.metadata_backend == "sqlite"
    assert s.partition_column == "pickup_date"


def test_env_overrides_are_applied(tmp_path):
    s = Settings.from_env({"DATATRUST_DATA_ROOT": str(tmp_path), "DATATRUST_PARTITION_SCHEME": "monthly",
                           "DATATRUST_UNEXPECTED_COLUMNS": "fail"})
    assert s.data_root == tmp_path
    assert s.partition_column == "pickup_month"
    assert s.unexpected_columns == "fail"


@pytest.mark.parametrize("key,value", [
    ("DATATRUST_PARTITION_SCHEME", "hourly"), ("DATATRUST_UNEXPECTED_COLUMNS", "maybe"),
    ("DATATRUST_FAIL_ON_SEVERITY", "FATAL"), ("DATATRUST_METADATA_BACKEND", "mysql"),
    ("DATATRUST_CSV_DELIMITER", ";;"), ("DATATRUST_SHUFFLE_PARTITIONS", "abc"),
])
def test_invalid_settings_raise(key, value):
    with pytest.raises(ConfigError):
        Settings.from_env({key: value})


def test_postgres_requires_password_and_never_falls_back():
    with pytest.raises(ConfigError):
        Settings.from_env({"DATATRUST_METADATA_BACKEND": "postgres"})


def test_safe_join_blocks_traversal(tmp_path):
    assert safe_join(tmp_path, "a", "b").is_relative_to(tmp_path.resolve())
    with pytest.raises(ConfigError):
        safe_join(tmp_path, "..", "etc")


@pytest.mark.parametrize("bad", ["../x", "a/b", "a b", "", "x;rm"])
def test_validate_safe_value_rejects_unsafe(bad):
    with pytest.raises(ConfigError):
        validate_safe_value(bad)
