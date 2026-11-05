import pytest

from src.cli import build_parser, main


def test_every_command_parses():
    parser = build_parser()
    for argv in (["ingest", "--input", "x"], ["run", "--force"], ["backfill", "--start", "2023-01-01", "--end", "2023-01-02"],
                 ["quality"], ["models"], ["dbt", "--dbt-command", "test"], ["status"], ["lineage"],
                 ["reset", "--yes"], ["step", "--step", "discover_files", "--run-id", "r1"],
                 ["generate-sample"], ["benchmark", "--rows", "10"], ["validate-config"], ["unlock"]):
        assert parser.parse_args(argv).func


def test_backfill_requires_dates():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["backfill"])


def test_validate_config_command_returns_zero(capsys):
    assert main(["validate-config"]) == 0
    assert "quality rules loaded" in capsys.readouterr().out


def test_generate_sample_command_writes_files(tmp_path, capsys):
    assert main(["generate-sample", "--out", str(tmp_path), "--months", "2023-05", "--rows", "50"]) == 0
    assert (tmp_path / "yellow_tripdata_2023-05_sample.csv").exists()


def test_reset_without_confirmation_deletes_nothing(tmp_path, monkeypatch):
    data = tmp_path / "data"
    (data / "silver" / "trips").mkdir(parents=True)
    marker = data / "silver" / "trips" / "keep.parquet"
    marker.write_text("x")
    monkeypatch.setenv("DATATRUST_DATA_ROOT", str(data))
    monkeypatch.setattr("builtins.input", lambda *_: "no")
    assert main(["reset"]) == 1
    assert marker.exists()


def test_reset_with_yes_clears_generated_data_but_keeps_samples(tmp_path, monkeypatch):
    data = tmp_path / "data"
    (data / "silver" / "trips").mkdir(parents=True)
    (data / "silver" / "trips" / "a.parquet").write_text("x")
    (data / "sample").mkdir()
    (data / "sample" / "mine.csv").write_text("a\n1\n")
    monkeypatch.setenv("DATATRUST_DATA_ROOT", str(data))
    assert main(["reset", "--yes"]) == 0
    assert not (data / "silver" / "trips" / "a.parquet").exists()
    assert (data / "sample" / "mine.csv").exists()
