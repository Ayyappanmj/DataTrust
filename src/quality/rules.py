"""Declarative data quality rules.

Each row-level rule is a *portable SQL boolean expression* over the typed Silver columns.
The same expression text is evaluated by Spark (``F.expr``) and by any DB-API SQL engine
(DuckDB, SQLite). A row FAILS a rule only when the expression is FALSE; NULL means
"not applicable" so each defect is counted by exactly one rule (e.g. an unparseable
pickup time is counted by ``pickup_ts_valid`` and not again by the range rule).
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from src.common.config import SEVERITIES, Settings
from src.common.exceptions import ConfigError

ACTIONS = ("reject", "flag")  # reject -> row is quarantined; flag -> row is kept but counted


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    expression: str
    columns: tuple[str, ...]
    severity: str = "ERROR"
    action: str = "reject"
    max_failure_pct: float = 5.0
    params: dict = field(default_factory=dict)
    kind: str = "expression"  # expression | duplicate | dataset
    enabled: bool = True

    def sql(self) -> str:
        return self.expression.format(**self.params) if self.params else self.expression

    def describe(self) -> str:
        return self.description.format(**self.params) if self.params else self.description


@dataclass
class RuleResult:
    dataset_name: str
    rule_name: str
    rule_description: str
    records_evaluated: int
    records_failed: int
    severity: str
    action: str
    max_failure_pct: float
    stage: str = "silver"
    run_id: str = ""
    executed_at: str = ""

    @property
    def records_passed(self) -> int:
        return self.records_evaluated - self.records_failed

    @property
    def failure_pct(self) -> float:
        if self.records_evaluated <= 0:
            return 0.0
        return round(100.0 * self.records_failed / self.records_evaluated, 4)

    @property
    def status(self) -> str:
        return status_for(self.records_failed, self.records_evaluated, self.max_failure_pct)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id, "stage": self.stage, "dataset_name": self.dataset_name,
            "rule_name": self.rule_name, "rule_description": self.rule_description,
            "severity": self.severity, "action": self.action,
            "records_evaluated": self.records_evaluated, "records_passed": self.records_passed,
            "records_failed": self.records_failed, "failure_pct": self.failure_pct,
            "threshold_pct": self.max_failure_pct, "status": self.status,
            "executed_at": self.executed_at,
        }


def status_for(failed: int, evaluated: int, max_failure_pct: float) -> str:
    """PASSED: no failures. WARN: failures within the tolerated threshold. FAILED: above it."""
    if failed <= 0:
        return "PASSED"
    pct = 100.0 * failed / evaluated if evaluated else 100.0
    return "WARN" if pct <= max_failure_pct else "FAILED"


_AMOUNT_SUM = (
    "fare_amount + COALESCE(extra, 0) + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0) "
    "+ COALESCE(tolls_amount, 0) + COALESCE(improvement_surcharge, 0) "
    "+ COALESCE(congestion_surcharge, 0) + COALESCE(airport_fee, 0) "
    "+ COALESCE(cbd_congestion_fee, 0)"
)


def _build_default_rules() -> list[Rule]:
    rules = [
        Rule("malformed_record", "Source line could not be parsed by the CSV reader.",
             "corrupt_record IS NULL", ("corrupt_record",), "ERROR", max_failure_pct=2.0),
        Rule("pickup_ts_valid", "Pickup timestamp is present and parseable.",
             "pickup_ts IS NOT NULL", ("pickup_ts",), "ERROR", max_failure_pct=2.0),
        Rule("dropoff_ts_valid", "Drop-off timestamp is present and parseable.",
             "dropoff_ts IS NOT NULL", ("dropoff_ts",), "ERROR", max_failure_pct=2.0),
        Rule("pickup_ts_in_range", "Pickup falls between {min_ts} and {max_ts}.",
             "pickup_ts >= '{min_ts}' AND pickup_ts < '{max_ts}'", ("pickup_ts",), "ERROR",
             params={"min_ts": "2009-01-01 00:00:00", "max_ts": "2100-01-01 00:00:00"}),
        Rule("pickup_before_dropoff", "Pickup is not after drop-off.",
             "pickup_ts <= dropoff_ts", ("pickup_ts", "dropoff_ts"), "ERROR"),
        Rule("trip_duration_valid", "Trip lasts more than 0 and at most {max_minutes} minutes.",
             "trip_duration_minutes > 0 AND trip_duration_minutes <= {max_minutes}",
             ("trip_duration_minutes",), "ERROR", params={"max_minutes": 1440}),
        Rule("pu_location_present", "Pickup location id is present.",
             "pu_location_id IS NOT NULL", ("pu_location_id",), "ERROR", max_failure_pct=2.0),
        Rule("do_location_present", "Drop-off location id is present.",
             "do_location_id IS NOT NULL", ("do_location_id",), "ERROR", max_failure_pct=2.0),
        Rule("fare_amount_present", "Fare amount is present and numeric.",
             "fare_amount IS NOT NULL", ("fare_amount",), "ERROR", max_failure_pct=2.0),
        Rule("total_amount_present", "Total amount is present and numeric.",
             "total_amount IS NOT NULL", ("total_amount",), "ERROR", max_failure_pct=2.0),
        Rule("fare_amount_non_negative", "Fare amount is not negative.",
             "fare_amount >= 0", ("fare_amount",), "ERROR"),
        Rule("tip_amount_non_negative", "Tip amount is not negative.",
             "tip_amount >= 0", ("tip_amount",), "ERROR"),
        Rule("tolls_amount_non_negative", "Tolls amount is not negative.",
             "tolls_amount >= 0", ("tolls_amount",), "ERROR"),
        Rule("total_amount_non_negative", "Total amount is not negative.",
             "total_amount >= 0", ("total_amount",), "ERROR"),
        Rule("passenger_count_valid",
             "Passenger count (when present) is between 1 and {max_passengers}.",
             "passenger_count BETWEEN 1 AND {max_passengers}", ("passenger_count",), "ERROR",
             params={"max_passengers": 9}),
        Rule("trip_distance_valid", "Trip distance is between 0 and {max_miles} miles.",
             "trip_distance >= 0 AND trip_distance <= {max_miles}", ("trip_distance",), "ERROR",
             params={"max_miles": 1000}),
        Rule("coordinates_valid_range",
             "Pickup coordinates (only when the columns exist) are valid lat/lon values.",
             "pickup_latitude BETWEEN -90 AND 90 AND pickup_longitude BETWEEN -180 AND 180",
             ("pickup_latitude", "pickup_longitude"), "ERROR"),
        Rule("coordinates_plausible_nyc",
             "Pickup coordinates (only when the columns exist) lie in the NYC area; "
             "(0,0) placeholders in old TLC files are flagged.",
             "pickup_latitude BETWEEN 40.4 AND 41.1 AND pickup_longitude BETWEEN -74.5 AND -73.5",
             ("pickup_latitude", "pickup_longitude"), "WARNING", action="flag",
             max_failure_pct=10.0),
        Rule("payment_type_known", "Payment type is one of the documented TLC codes 0-6.",
             "payment_type IN (0, 1, 2, 3, 4, 5, 6)", ("payment_type",), "WARNING",
             action="flag", max_failure_pct=5.0),
        Rule("total_reconciles",
             "Total amount equals the sum of its components within {tolerance}.",
             f"ABS(total_amount - ({_AMOUNT_SUM})) <= {{tolerance}}",
             ("total_amount", "fare_amount"), "WARNING", action="flag", max_failure_pct=10.0,
             params={"tolerance": 0.05}),
        Rule("duplicate_record", "Record is not a duplicate of another accepted record.",
             "", ("record_id",), "ERROR", kind="duplicate"),
        Rule("rejection_rate_acceptable",
             "Share of rejected records stays below the dataset-level limit.",
             "", (), "CRITICAL", kind="dataset", max_failure_pct=30.0),
    ]
    return rules


def _build_file_rules() -> list[Rule]:
    """File-level checks evaluated during ingestion (evaluated = files or rows, see runner)."""
    return [
        Rule("source_file_readable", "Source file exists, is non-empty and is not corrupted.",
             "", (), "CRITICAL", kind="dataset", max_failure_pct=0.0),
        Rule("required_columns_present", "Every required column exists in the source file.",
             "", (), "CRITICAL", kind="dataset", max_failure_pct=0.0),
        Rule("unexpected_columns", "Source file has no columns outside the known schema.",
             "", (), "WARNING", kind="dataset", max_failure_pct=0.0),
        Rule("schema_drift", "Column set matches the previously ingested file.",
             "", (), "WARNING", kind="dataset", max_failure_pct=0.0),
        Rule("malformed_lines", "CSV lines have the same number of fields as the header.",
             "", (), "WARNING", kind="dataset", max_failure_pct=1.0),
    ]


DEFAULT_RULES: list[Rule] = _build_default_rules()
FILE_RULES: list[Rule] = _build_file_rules()
FILE_RULE_INDEX = {r.name: r for r in FILE_RULES}
RULE_INDEX = {rule.name: rule for rule in DEFAULT_RULES}


def load_rules(settings: Settings | None = None, config_path: Path | None = None) -> list[Rule]:
    """Return the default rules with overrides from config/quality_rules.yaml applied."""
    rules = {rule.name: rule for rule in DEFAULT_RULES}
    path = config_path or (settings.quality_config if settings else None)
    if settings is not None and "rejection_rate_acceptable" in rules:
        rules["rejection_rate_acceptable"] = replace(
            rules["rejection_rate_acceptable"], max_failure_pct=settings.max_rejection_rate_pct
        )
    if path is None or not Path(path).is_file():
        return list(rules.values())
    import yaml

    try:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    overrides = doc.get("rules") or {}
    if not isinstance(overrides, dict):
        raise ConfigError("'rules' in quality config must be a mapping")
    for name, spec in overrides.items():
        if name not in rules:
            raise ConfigError(f"Unknown rule in quality config: {name!r}")
        if not isinstance(spec, dict):
            raise ConfigError(f"Rule {name!r} override must be a mapping")
        unknown = set(spec) - {"severity", "action", "max_failure_pct", "enabled", "params"}
        if unknown:
            raise ConfigError(f"Rule {name!r} has unknown keys: {sorted(unknown)}")
        current = rules[name]
        changes: dict = {}
        if "severity" in spec:
            if str(spec["severity"]).upper() not in SEVERITIES:
                raise ConfigError(f"Rule {name!r}: invalid severity {spec['severity']!r}")
            changes["severity"] = str(spec["severity"]).upper()
        if "action" in spec:
            if spec["action"] not in ACTIONS:
                raise ConfigError(f"Rule {name!r}: action must be one of {ACTIONS}")
            changes["action"] = spec["action"]
        if "max_failure_pct" in spec:
            value = float(spec["max_failure_pct"])
            if not 0 <= value <= 100:
                raise ConfigError(f"Rule {name!r}: max_failure_pct must be within 0-100")
            changes["max_failure_pct"] = value
        if "enabled" in spec:
            changes["enabled"] = bool(spec["enabled"])
        if "params" in spec:
            bad = set(spec["params"]) - set(current.params)
            if bad:
                raise ConfigError(f"Rule {name!r}: unknown params {sorted(bad)}")
            changes["params"] = {**current.params, **spec["params"]}
        rules[name] = replace(current, **changes)
    return list(rules.values())


def applicable(rules: list[Rule], available_columns: set[str]) -> list[Rule]:
    """Enabled rules whose required columns all exist."""
    return [r for r in rules if r.enabled and set(r.columns) <= available_columns]
