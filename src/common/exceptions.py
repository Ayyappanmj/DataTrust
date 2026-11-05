"""Project-specific exceptions. Each one maps to a distinct, diagnosable failure."""


class DataTrustError(Exception):
    """Base class for every error raised deliberately by DataTrust."""


class ConfigError(DataTrustError):
    """Invalid or missing configuration."""


class FileValidationError(DataTrustError):
    """A source file is missing, unreadable, empty or corrupted."""


class SchemaValidationError(DataTrustError):
    """A source file does not match the expected schema under the configured policy."""


class QualityGateError(DataTrustError):
    """A data quality rule at or above the configured severity failed its threshold."""


class MetadataError(DataTrustError):
    """Problem reading or writing pipeline metadata."""


class MetadataConnectionError(MetadataError):
    """The selected metadata backend (e.g. PostgreSQL) cannot be reached."""


class ProcessingError(DataTrustError):
    """A processing step (Spark job, partition commit, ...) failed."""


class WarehouseError(DataTrustError):
    """Loading or querying the DuckDB warehouse failed."""


class ModelingError(DataTrustError):
    """Building analytical models (SQL or dbt) failed."""


class PipelineLockedError(DataTrustError):
    """Another pipeline run appears to be active."""


class InjectedFault(DataTrustError):
    """Raised on purpose by the fault-injection hook (tests and recovery demos only)."""
