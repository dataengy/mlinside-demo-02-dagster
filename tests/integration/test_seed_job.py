"""demo_prepare_job: seed snapshot → временная DuckDB, 8 таблиц в raw, повтор = те же counts."""

from __future__ import annotations

import csv
import os
from pathlib import Path

import duckdb
import pytest

from olist_ml.sampling import RAW_TABLES
from olist_ml.settings import settings

pytestmark = pytest.mark.integration

SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"


def _csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def _raw_counts(db: Path) -> dict[str, int]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {t: con.execute(f"select count(*) from raw.{t}").fetchone()[0] for t in RAW_TABLES}
    finally:
        con.close()


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_seed_job_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "olist.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db))  # dbt читает profiles через env_var
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    from olist_ml.definitions import defs

    job = defs().resolve_job_def("demo_prepare_job")
    assert job.execute_in_process().success
    first = _raw_counts(db)
    expected = {t: _csv_rows(SEEDS_DIR / f"{t}.csv") for t in RAW_TABLES}
    assert first == expected
    assert job.execute_in_process().success
    assert _raw_counts(db) == first
    assert os.environ["DUCKDB_PATH"] == str(db)
