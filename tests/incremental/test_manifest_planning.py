from src.ingestion.file_discovery import discover_files
from src.ingestion.ingestion_manifest import mark_failed, mark_ingested, mark_processed, plan_files, register_file
from src.processing.incremental_processing import build_workset


def _discover(path):
    files, _ = discover_files(path)
    return files


def _process(repo, entry, run="r1"):
    register_file(repo, entry, run)
    mark_ingested(repo, entry.checksum, 10, '["a"]', "fp", "/b", run)
    mark_processed(repo, entry, run)


def test_new_file_needs_everything(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    assert (entry.action, entry.needs_ingest, entry.needs_process) == ("NEW", True, True)


def test_unchanged_file_is_skipped_after_processing(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    _process(repo, entry)
    [again] = plan_files(repo, _discover(tmp_path))
    assert again.action == "UNCHANGED" and again.skipped
    assert build_workset([again], set()).empty


def test_changed_file_supersedes_previous_version(repo, tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("a\n1\n")
    [first] = plan_files(repo, _discover(tmp_path))
    _process(repo, first)
    f.write_text("a\n1\n2\n")
    [second] = plan_files(repo, _discover(tmp_path))
    assert second.action == "CHANGED" and second.supersedes == first.checksum
    work = build_workset([second], set())
    assert first.checksum in work.remove_checksums
    _process(repo, second, "r2")
    assert not repo.get_manifest(first.checksum)["active"]
    assert repo.get_manifest(second.checksum)["active"]


def test_incomplete_attempt_is_retried_not_skipped(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    register_file(repo, entry, "r1")
    mark_ingested(repo, entry.checksum, 10, '["a"]', "fp", "/b", "r1")  # silver never finished
    [retry] = plan_files(repo, _discover(tmp_path))
    assert retry.action == "RETRY" and not retry.needs_ingest and retry.needs_process and retry.refresh


def test_failed_ingest_is_retried_with_ingest(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    register_file(repo, entry, "r1")
    mark_failed(repo, entry.checksum, "ingest", "boom", "r1")
    [retry] = plan_files(repo, _discover(tmp_path))
    assert retry.action == "RETRY" and retry.needs_ingest


def test_force_reprocesses_unchanged_file_and_refreshes_rows(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    _process(repo, entry)
    [forced] = plan_files(repo, _discover(tmp_path), force=True)
    assert forced.action == "FORCE" and forced.refresh
    assert forced.checksum in build_workset([forced], set()).remove_checksums


def test_duplicate_content_under_another_name_is_skipped(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    (tmp_path / "copy_of_a.csv").write_text("a\n1\n")
    entries = plan_files(repo, _discover(tmp_path))
    assert sorted(e.action for e in entries) == ["DUPLICATE_CONTENT", "NEW"]
    _process(repo, next(e for e in entries if e.action == "NEW"))
    later = plan_files(repo, _discover(tmp_path))
    assert {e.action for e in later} <= {"UNCHANGED", "DUPLICATE_CONTENT"}


def test_reverting_a_file_to_an_old_version_reactivates_it(repo, tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("a\n1\n")
    [v1] = plan_files(repo, _discover(tmp_path))
    _process(repo, v1)
    f.write_text("a\n2\n")
    [v2] = plan_files(repo, _discover(tmp_path))
    _process(repo, v2, "r2")
    f.write_text("a\n1\n")
    [back] = plan_files(repo, _discover(tmp_path))
    assert back.action == "REACTIVATE" and back.supersedes == v2.checksum and back.needs_process


def test_failed_files_are_excluded_from_workset(repo, tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    [entry] = plan_files(repo, _discover(tmp_path))
    assert build_workset([entry], {entry.checksum}).empty
