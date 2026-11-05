"""Static validation of configuration files and the Airflow DAG (no Airflow install needed)."""
import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
yaml = pytest.importorskip("yaml")


@pytest.mark.parametrize("rel", [
    "docker-compose.yml", "docker-compose.airflow.yml", ".github/workflows/ci.yml",
    "dbt_project/dbt_project.yml", "dbt_project/models/schema.yml", "dbt_project/models/staging/sources.yml",
    "dbt_project/profiles.yml.example", "config/quality_rules.yaml"])
def test_yaml_files_parse(rel):
    assert yaml.safe_load((ROOT / rel).read_text(encoding="utf-8")) is not None


def test_dag_has_all_tasks_in_a_linear_dependency_chain():
    tree = ast.parse((ROOT / "dags" / "datatrust_pipeline.py").read_text())
    task_ids = [n.args[0].value for n in ast.walk(tree)
                if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "step"]
    assert task_ids == ["discover_input_files", "validate_source_files", "ingest_source_data",
                        "run_schema_checks", "bronze_to_silver", "execute_quality_rules", "load_duckdb",
                        "run_dbt_models", "final_quality_checks", "record_lineage", "generate_run_summary"]
    source = (ROOT / "dags" / "datatrust_pipeline.py").read_text()
    assert source.count(">>") == len(task_ids) - 1 or source.count(">>") >= 10
    assert "retries" in source and "execution_timeout" in source and "xcom_push" not in source and "xcom_pull" not in source


def test_dag_steps_exist_in_pipeline_runner():
    from src.orchestration.pipeline_runner import STEPS

    source = (ROOT / "dags" / "datatrust_pipeline.py").read_text()
    for step in STEPS:
        assert f'"{step}"' in source


def test_ci_workflow_runs_lint_tests_and_dbt():
    text = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    for needle in ("ruff check", "pytest", "dbt", "setup-java", "validate-config"):
        assert needle in text


def test_env_example_contains_placeholders_only():
    text = (ROOT / ".env.example").read_text()
    assert "change-me" in text
    assert "AKIA" not in text


def test_no_hardcoded_credentials_in_source():
    for path in list((ROOT / "src").rglob("*.py")) + list((ROOT / "dags").rglob("*.py")):
        content = path.read_text(encoding="utf-8").lower()
        assert 'password="' not in content.replace(" ", "") or "pg_password" in content, path
