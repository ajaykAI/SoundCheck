"""Validate restored DuckDB checkpoints and recover damaged artist-resolution state."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from soundcheck.sql.loader import load_sql

_REBUILDABLE_TABLES = frozenset(
    {
        "stg_.post_artist_links",
        "stg_.post_artist_ambiguities",
        "stg_.post_artist_resolution_attempts",
    }
)
_CORRUPTION_MARKERS = (
    "corrupted rle segment",
    "failed to scan dictionary string",
    "invalid bitpacking mode",
)


class TableFingerprint(BaseModel):
    """A full-column scan, including nested values, without logging source data."""

    model_config = ConfigDict(frozen=True)

    rows: int = Field(ge=0)
    column_hashes: tuple[int | None, ...]


class CheckpointReport(BaseModel):
    """Validation and recovery result consumed by scheduled automation."""

    model_config = ConfigDict(frozen=True)

    event: str = "checkpoint_checked"
    tables_checked: int = Field(ge=0)
    resolution_rebuilt: bool = False
    damaged_tables: tuple[str, ...] = ()
    backup_path: str | None = None


def _table_names(database: Path) -> tuple[str, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = connection.execute(load_sql("inspect_checkpoint_tables.sql")).fetchall()
    return tuple(f"{schema}.{table}" for schema, table in rows)


def _scan_table(database: Path, table: str) -> TableFingerprint:
    # An internal storage error invalidates its connection. Isolate each scan so
    # later tables are checked with a fresh connection rather than false failures.
    with duckdb.connect(str(database), read_only=True) as connection:
        row = connection.execute(load_sql("inspect_checkpoint_scan.sql"), [table]).fetchone()
    if row is None:
        raise RuntimeError(f"Checkpoint scan returned no result for {table}")
    return TableFingerprint(rows=row[0], column_hashes=tuple(row[1:]))


def check_checkpoint(database: Path, *, repair_resolution: bool = False) -> CheckpointReport:
    """Fail closed on unreadable data; optionally rebuild only known derived tables.

    Recovery is prepared on a separate copy, fully rescanned, and atomically
    installed only after every unaffected table has the same row count and
    column fingerprints. The original checkpoint is retained alongside it.
    """
    if not database.exists():
        raise FileNotFoundError(database)
    if Path(str(database) + ".wal").exists():
        raise RuntimeError("Checkpoint has a WAL; close its writer before validation")
    tables = _table_names(database)
    fingerprints: dict[str, TableFingerprint] = {}
    damaged: list[str] = []
    for table in tables:
        try:
            fingerprints[table] = _scan_table(database, table)
        except duckdb.Error as exc:
            known_corruption = any(marker in str(exc).lower() for marker in _CORRUPTION_MARKERS)
            if not repair_resolution or table not in _REBUILDABLE_TABLES or not known_corruption:
                raise RuntimeError(f"Checkpoint validation failed for {table}: {exc}") from exc
            damaged.append(table)

    if not damaged:
        return CheckpointReport(tables_checked=len(tables))

    backup = database.with_name(database.name + ".before-resolution-repair")
    if backup.exists():
        raise FileExistsError(f"Preserving existing recovery backup: {backup}")
    with TemporaryDirectory(prefix="checkpoint-repair-", dir=database.parent) as directory:
        candidate = Path(directory) / database.name
        shutil.copy2(database, candidate)
        with duckdb.connect(str(candidate)) as connection:
            connection.begin()
            connection.execute(load_sql("reset_corrupt_artist_resolution.sql"))
            connection.execute(load_sql("create_stg_post_artist_resolution.sql"))
            connection.commit()

        candidate_tables = _table_names(candidate)
        if set(candidate_tables) != set(tables) | _REBUILDABLE_TABLES:
            raise RuntimeError("Recovery changed the checkpoint table inventory")
        for table in candidate_tables:
            actual = _scan_table(candidate, table)
            if table in _REBUILDABLE_TABLES:
                if actual.rows != 0:
                    raise RuntimeError(f"Recovery left stale resolution state in {table}")
            elif actual != fingerprints[table]:
                raise RuntimeError(f"Recovery changed preserved data in {table}")

        # Keep a recoverable original before replacing the working checkpoint.
        shutil.copy2(database, backup)
        candidate.replace(database)

    return CheckpointReport(
        tables_checked=len(candidate_tables),
        resolution_rebuilt=True,
        damaged_tables=tuple(damaged),
        backup_path=str(backup),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/soundcheck.duckdb"))
    parser.add_argument("--repair-resolution", action="store_true")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    report = check_checkpoint(args.database, repair_resolution=args.repair_resolution)
    print(json.dumps(report.model_dump(mode="json"), sort_keys=True))
    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"resolution_rebuilt={str(report.resolution_rebuilt).lower()}\n")


if __name__ == "__main__":
    main()
