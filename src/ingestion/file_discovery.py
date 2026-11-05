"""Find input files, compute checksums and validate basic file integrity."""
from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass, field
from pathlib import Path

from src.common.exceptions import FileValidationError
from src.processing.schema_validation import normalize_columns

SUPPORTED_EXTENSIONS = {".csv": "csv", ".parquet": "parquet"}
CHECKSUM_KEY_LEN = 20


@dataclass(frozen=True)
class DiscoveredFile:
    path: Path
    file_format: str
    size_bytes: int
    checksum: str

    @property
    def key(self) -> str:
        """Short checksum used in directory names (full checksum stays in the manifest)."""
        return short_checksum(self.checksum)


@dataclass(frozen=True)
class SkippedFile:
    path: Path
    reason: str


@dataclass
class FileValidation:
    ok: bool
    columns: list[str] = field(default_factory=list)
    row_count: int | None = None
    malformed_lines: int = 0
    error: str = ""


def short_checksum(checksum: str) -> str:
    return checksum[:CHECKSUM_KEY_LEN]


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    """Stream the file through SHA-256 (constant memory)."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_files(
    input_path: Path, recursive: bool = True
) -> tuple[list[DiscoveredFile], list[SkippedFile]]:
    """List supported files under ``input_path`` (a directory, or a single file).

    Directories/files starting with ``_`` or ``.`` are ignored (e.g. ``_expected``).
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileValidationError(f"Input path does not exist: {input_path}")
    candidates = [input_path] if input_path.is_file() else (
        sorted(input_path.rglob("*") if recursive else input_path.glob("*"))
    )
    found: list[DiscoveredFile] = []
    skipped: list[SkippedFile] = []
    for path in candidates:
        if not path.is_file():
            continue
        relative_parts = path.relative_to(input_path).parts if input_path.is_dir() else (path.name,)
        if any(part.startswith(("_", ".")) for part in relative_parts):
            continue
        fmt = SUPPORTED_EXTENSIONS.get(path.suffix.lower())
        if fmt is None:
            skipped.append(SkippedFile(path, f"unsupported extension {path.suffix!r}"))
            continue
        found.append(DiscoveredFile(path.resolve(), fmt, path.stat().st_size, sha256_file(path)))
    return found, skipped


def validate_csv(path: Path, delimiter: str = ",") -> FileValidation:
    """Check encoding, header, duplicate columns and count rows with the wrong field count."""
    if path.stat().st_size == 0:
        return FileValidation(False, error="file is empty")
    malformed = rows = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            header = next(reader, None)
            if not header or all(not c.strip() for c in header):
                return FileValidation(False, error="missing header row")
            normalize_columns(header)  # raises on colliding names
            width = len(header)
            for row in reader:
                if not row:
                    continue
                rows += 1
                if len(row) != width:
                    malformed += 1
    except UnicodeDecodeError as exc:
        return FileValidation(False, error=f"not valid UTF-8 text (corrupted?): {exc.reason}")
    except csv.Error as exc:
        return FileValidation(False, error=f"CSV parse error: {exc}")
    except Exception as exc:  # noqa: BLE001 - surfaced as a validation failure with the message
        return FileValidation(False, error=f"{type(exc).__name__}: {exc}")
    if rows == 0:
        return FileValidation(False, columns=header, row_count=0, error="file has a header but no rows")
    return FileValidation(True, columns=header, row_count=rows, malformed_lines=malformed)


def validate_parquet(path: Path) -> FileValidation:
    """Open the Parquet footer with pyarrow (detects truncated or corrupted files)."""
    try:
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        columns = list(parquet.schema_arrow.names)
        rows = parquet.metadata.num_rows
    except ImportError as exc:
        return FileValidation(False, error=f"pyarrow is required to validate Parquet: {exc}")
    except Exception as exc:  # noqa: BLE001
        return FileValidation(False, error=f"unreadable Parquet file: {exc}")
    if rows == 0:
        return FileValidation(False, columns=columns, row_count=0, error="Parquet file has no rows")
    return FileValidation(True, columns=columns, row_count=rows)


def validate_file(file: DiscoveredFile, delimiter: str = ",") -> FileValidation:
    if not file.path.is_file():
        return FileValidation(False, error="file disappeared before validation")
    if file.file_format == "csv":
        return validate_csv(file.path, delimiter)
    return validate_parquet(file.path)
