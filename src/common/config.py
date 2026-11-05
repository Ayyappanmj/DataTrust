"""Central configuration. Everything is driven by environment variables (or a .env file)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path

from src.common.exceptions import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEVERITIES = ("INFO", "WARNING", "ERROR", "CRITICAL")
SEVERITY_RANK = {name: rank for rank, name in enumerate(SEVERITIES)}
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.\-]+$")

DEFAULT_TIMESTAMP_FORMATS = (
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",
)


def load_dotenv(path: Path | None = None) -> dict[str, str]:
    """Parse a simple KEY=VALUE file. Real environment variables always win."""
    path = path or PROJECT_ROOT / ".env"
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")
        values[key.strip()] = value
    return values


def validate_safe_value(value: str, what: str = "value") -> str:
    """Reject path-like or unusual strings before they are used in a file path."""
    if not _SAFE_VALUE.match(value) or ".." in value:
        raise ConfigError(f"Unsafe {what}: {value!r}")
    return value


def safe_join(base: Path, *parts: str) -> Path:
    """Join `parts` to `base` and make sure the result cannot escape `base`."""
    base_resolved = base.resolve()
    target = base_resolved.joinpath(*parts).resolve()
    if base_resolved != target and base_resolved not in target.parents:
        raise ConfigError(f"Path escapes base directory: {target}")
    return target


@dataclass(frozen=True)
class Settings:
    """Immutable settings object. Use `dataclasses.replace` to derive variants in tests."""

    data_root: Path = PROJECT_ROOT / "data"
    metadata_backend: str = "sqlite"
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "datatrust"
    pg_user: str = "datatrust"
    pg_password: str = ""
    spark_master: str = "local[2]"
    spark_driver_memory: str = "2g"
    shuffle_partitions: int = 8
    partition_scheme: str = "daily"
    unexpected_columns: str = "warn"
    fail_on_severity: str = "CRITICAL"
    on_file_error: str = "continue"
    csv_delimiter: str = ","
    timestamp_formats: tuple[str, ...] = DEFAULT_TIMESTAMP_FORMATS
    modeling_engine: str = "duckdb"
    log_level: str = "INFO"
    log_json: bool = False
    quality_config: Path = PROJECT_ROOT / "config" / "quality_rules.yaml"
    dbt_project_dir: Path = PROJECT_ROOT / "dbt_project"
    max_rejection_rate_pct: float = 30.0
    lock_timeout_minutes: int = 120
    fault_injection: str = ""  # e.g. "fail_at=bronze_to_silver" (tests / recovery demos only)
    extra: dict = field(default_factory=dict)

    # ---- derived paths -------------------------------------------------
    @property
    def sample_dir(self) -> Path:
        return self.data_root / "sample"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.data_root / "bronze" / "trips"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver" / "trips"

    @property
    def gold_dir(self) -> Path:
        return self.data_root / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_root / "quarantine" / "trips"

    @property
    def reports_dir(self) -> Path:
        return self.data_root / "reports"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "_staging"

    @property
    def state_dir(self) -> Path:
        return self.data_root / "_state"

    @property
    def trash_dir(self) -> Path:
        return self.data_root / "_trash"

    @property
    def lock_path(self) -> Path:
        return self.data_root / ".pipeline.lock"

    @property
    def sqlite_path(self) -> Path:
        return self.data_root / "metadata" / "datatrust_metadata.db"

    @property
    def warehouse_path(self) -> Path:
        return self.data_root / "warehouse" / "datatrust.duckdb"

    @property
    def partition_column(self) -> str:
        return "pickup_date" if self.partition_scheme == "daily" else "pickup_month"

    # ---- helpers -------------------------------------------------------
    def pg_dsn(self) -> str:
        if not self.pg_password:
            raise ConfigError("DATATRUST_PG_PASSWORD is required when the backend is postgres")
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    def validate(self) -> None:
        """Raise ConfigError for any invalid setting."""
        choices = {
            "metadata_backend": ("sqlite", "postgres"),
            "partition_scheme": ("daily", "monthly"),
            "unexpected_columns": ("warn", "fail", "ignore"),
            "fail_on_severity": SEVERITIES,
            "on_file_error": ("continue", "abort"),
            "modeling_engine": ("duckdb", "dbt"),
        }
        for name, allowed in choices.items():
            value = getattr(self, name)
            if value not in allowed:
                raise ConfigError(f"{name}={value!r} is invalid; choose one of {allowed}")
        if len(self.csv_delimiter) != 1:
            raise ConfigError("csv_delimiter must be exactly one character")
        if self.shuffle_partitions < 1:
            raise ConfigError("shuffle_partitions must be >= 1")
        if not 0 < self.max_rejection_rate_pct <= 100:
            raise ConfigError("max_rejection_rate_pct must be in (0, 100]")
        if self.metadata_backend == "postgres" and not self.pg_password:
            raise ConfigError("postgres backend selected but DATATRUST_PG_PASSWORD is empty")

    def ensure_dirs(self) -> None:
        for path in (
            self.bronze_dir, self.silver_dir, self.quarantine_dir, self.gold_dir,
            self.reports_dir, self.sqlite_path.parent, self.warehouse_path.parent,
            self.staging_dir, self.state_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def with_overrides(self, **kwargs) -> Settings:
        return replace(self, **kwargs)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        merged = {**load_dotenv(), **(env if env is not None else os.environ)}

        def get(key: str, default: str) -> str:
            return merged.get(f"DATATRUST_{key}", default)

        def get_int(key: str, default: int) -> int:
            raw = get(key, str(default))
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigError(f"DATATRUST_{key} must be an integer, got {raw!r}") from exc

        root = Path(get("DATA_ROOT", str(PROJECT_ROOT / "data")))
        if not root.is_absolute():
            root = (PROJECT_ROOT / root).resolve()
        settings = cls(
            data_root=root,
            metadata_backend=get("METADATA_BACKEND", "sqlite").lower(),
            pg_host=get("PG_HOST", "localhost"),
            pg_port=get_int("PG_PORT", 5432),
            pg_db=get("PG_DB", "datatrust"),
            pg_user=get("PG_USER", "datatrust"),
            pg_password=get("PG_PASSWORD", ""),
            spark_master=get("SPARK_MASTER", "local[2]"),
            spark_driver_memory=get("SPARK_DRIVER_MEMORY", "2g"),
            shuffle_partitions=get_int("SHUFFLE_PARTITIONS", 8),
            partition_scheme=get("PARTITION_SCHEME", "daily").lower(),
            unexpected_columns=get("UNEXPECTED_COLUMNS", "warn").lower(),
            fail_on_severity=get("FAIL_ON_SEVERITY", "CRITICAL").upper(),
            on_file_error=get("ON_FILE_ERROR", "continue").lower(),
            csv_delimiter=get("CSV_DELIMITER", ","),
            modeling_engine=get("MODELING_ENGINE", "duckdb").lower(),
            log_level=get("LOG_LEVEL", "INFO").upper(),
            log_json=get("LOG_JSON", "false").lower() in {"1", "true", "yes"},
            fault_injection=get("FAULT_INJECTION", ""),
        )
        settings.validate()
        return settings


def get_settings() -> Settings:
    """Load settings from the environment (re-read on every call; cheap)."""
    return Settings.from_env()
