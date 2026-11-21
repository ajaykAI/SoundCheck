"""Checkpoint recovery preserves source history and fails closed outside derived state."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from soundcheck.scripts import check_checkpoint as checkpoint
from soundcheck.scripts.pipeline_manifest import begin_pipeline_run
from soundcheck.sql.loader import load_sql


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "checkpoint.duckdb"
    now = datetime(2026, 11, 21, tzinfo=UTC)
    begin_pipeline_run(
        path, run_id="test", run_kind="daily", trigger_name="test", git_sha=None, started_at=now
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            load_sql("insert_raw_lastfm_artist_snapshots.sql"),
            ("Artist", None, 10, 100, ["indie"], ["indie"], now),
        )
        connection.execute(
            load_sql("insert_stg_post_artist_attempts.sql"),
            ("at://test/post", "no_candidate", 0, now),
        )
    return path


def _simulate_corruption(monkeypatch: pytest.MonkeyPatch, database: Path, table: str) -> None:
    original_scan = checkpoint._scan_table

    def scan(path: Path, name: str) -> checkpoint.TableFingerprint:
        if path == database and name == table:
            raise duckdb.IOException("Corrupted RLE segment: invalid header")
        return original_scan(path, name)

    monkeypatch.setattr(checkpoint, "_scan_table", scan)


def test_healthy_checkpoint_is_not_modified(database: Path) -> None:
    original = database.read_bytes()
    report = checkpoint.check_checkpoint(database, repair_resolution=True)
    assert report.tables_checked > 0
    assert not report.resolution_rebuilt
    assert database.read_bytes() == original
    assert not list(database.parent.glob("*.before-resolution-repair"))


def test_recovery_keeps_raw_snapshots_and_original_backup(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = database.read_bytes()
    raw_before = checkpoint._scan_table(database, "raw_.lastfm_artist_snapshots")
    _simulate_corruption(monkeypatch, database, "stg_.post_artist_links")

    report = checkpoint.check_checkpoint(database, repair_resolution=True)

    assert report.resolution_rebuilt
    assert report.damaged_tables == ("stg_.post_artist_links",)
    assert report.backup_path is not None
    assert Path(report.backup_path).read_bytes() == original
    assert checkpoint._scan_table(database, "raw_.lastfm_artist_snapshots") == raw_before
    # Attempts are rebuilt together with links, so every post remains eligible.
    assert checkpoint._scan_table(database, "stg_.post_artist_resolution_attempts").rows == 0


@pytest.mark.parametrize("table", ["raw_.lastfm_artist_snapshots", "mart_.pipeline_runs"])
def test_non_resolution_corruption_never_modifies_database(
    database: Path, monkeypatch: pytest.MonkeyPatch, table: str
) -> None:
    original = database.read_bytes()
    _simulate_corruption(monkeypatch, database, table)
    with pytest.raises(RuntimeError, match="Checkpoint validation failed"):
        checkpoint.check_checkpoint(database, repair_resolution=True)
    assert database.read_bytes() == original


def test_validation_does_not_implicitly_repair(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = database.read_bytes()
    _simulate_corruption(monkeypatch, database, "stg_.post_artist_links")
    with pytest.raises(RuntimeError, match="Checkpoint validation failed"):
        checkpoint.check_checkpoint(database)
    assert database.read_bytes() == original


def test_unknown_database_error_is_not_treated_as_corruption(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def scan(path: Path, table: str) -> checkpoint.TableFingerprint:
        raise duckdb.IOException("Permission denied")

    monkeypatch.setattr(checkpoint, "_scan_table", scan)
    with pytest.raises(RuntimeError, match="Permission denied"):
        checkpoint.check_checkpoint(database, repair_resolution=True)


def test_changed_source_data_aborts_recovery(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = database.read_bytes()
    original_scan = checkpoint._scan_table

    def scan(path: Path, table: str) -> checkpoint.TableFingerprint:
        if path == database and table == "stg_.post_artist_links":
            raise duckdb.InternalException("Invalid bitpacking mode")
        if path != database and table == "raw_.lastfm_artist_snapshots":
            return checkpoint.TableFingerprint(rows=0, column_hashes=())
        return original_scan(path, table)

    monkeypatch.setattr(checkpoint, "_scan_table", scan)
    with pytest.raises(RuntimeError, match="Recovery changed preserved data"):
        checkpoint.check_checkpoint(database, repair_resolution=True)
    assert database.read_bytes() == original
    assert not list(database.parent.glob("*.before-resolution-repair"))


def test_missing_database_does_not_create_empty_checkpoint(tmp_path: Path) -> None:
    database = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        checkpoint.check_checkpoint(database, repair_resolution=True)
    assert not database.exists()


def test_checkpoint_with_wal_is_not_copied(database: Path) -> None:
    Path(str(database) + ".wal").touch()
    with pytest.raises(RuntimeError, match="Checkpoint has a WAL"):
        checkpoint.check_checkpoint(database, repair_resolution=True)
