import hashlib

from src.ingestion.file_discovery import discover_files, sha256_file, validate_csv, validate_file


def test_checksum_matches_hashlib(tmp_path):
    f = tmp_path / "a.csv"
    f.write_bytes(b"hello,world\n1,2\n")
    assert sha256_file(f) == hashlib.sha256(b"hello,world\n1,2\n").hexdigest()


def test_checksum_changes_when_content_changes(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("a,b\n1,2\n")
    first = sha256_file(f)
    f.write_text("a,b\n1,3\n")
    assert sha256_file(f) != first


def test_discovery_finds_supported_and_ignores_underscore_dirs(tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n")
    (tmp_path / "b.parquet").write_bytes(b"PAR1")
    (tmp_path / "notes.txt").write_text("x")
    (tmp_path / "_expected").mkdir()
    (tmp_path / "_expected" / "c.csv").write_text("a\n1\n")
    found, skipped = discover_files(tmp_path)
    assert sorted(f.path.name for f in found) == ["a.csv", "b.parquet"]
    assert [s.path.name for s in skipped] == ["notes.txt"]
    assert all(len(f.checksum) == 64 for f in found)


def test_missing_input_raises(tmp_path):
    import pytest

    from src.common.exceptions import FileValidationError

    with pytest.raises(FileValidationError):
        discover_files(tmp_path / "nope")


def test_valid_csv_reports_columns_rows_and_malformed_lines(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("a,b,c\n1,2,3\n4,5\n6,7,8\n")
    v = validate_csv(f)
    assert v.ok and v.columns == ["a", "b", "c"]
    assert v.row_count == 3 and v.malformed_lines == 1


def test_corrupted_binary_csv_is_invalid(tmp_path):
    f = tmp_path / "bad.csv"
    f.write_bytes(bytes(range(128, 256)) * 10)
    v = validate_csv(f)
    assert not v.ok and "UTF-8" in v.error


def test_empty_and_header_only_csv_are_invalid(tmp_path):
    empty = tmp_path / "e.csv"
    empty.write_text("")
    header_only = tmp_path / "h.csv"
    header_only.write_text("a,b\n")
    assert not validate_csv(empty).ok
    assert "no rows" in validate_csv(header_only).error


def test_duplicate_normalised_columns_are_rejected(tmp_path):
    f = tmp_path / "d.csv"
    f.write_text("Airport_fee,airport_fee\n1,2\n")
    v = validate_csv(f)
    assert not v.ok and "normalise" in v.error


def test_validate_file_flags_vanished_file(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("a\n1\n")
    found, _ = discover_files(tmp_path)
    f.unlink()
    assert not validate_file(found[0]).ok
