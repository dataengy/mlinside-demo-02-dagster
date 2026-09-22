"""clean_raw / clean_derived / clean_all (ADR-10): идемпотентны на пустом состоянии, чистят заполненное,
после clean_all → demo_prepare состояние воспроизводится (те же counts, версия 1)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import mlflow
import pytest

from olist_ml.settings import settings

pytestmark = pytest.mark.integration

SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"


def _job(name: str):
    from olist_ml.definitions import defs

    return defs().resolve_job_def(name)


def _schemas(db: Path) -> set[str]:
    if not db.exists():
        return set()
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {
            r[0]
            for r in con.execute("select schema_name from information_schema.schemata").fetchall()
        }
    finally:
        con.close()


def _count(db: Path, sql: str) -> int:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def test_clean_jobs_on_empty_state_succeed(tmp_path: Path) -> None:
    for name in ("clean_raw_job", "clean_derived_job", "clean_all_job"):
        assert _job(name).execute_in_process().success, name


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_clean_levels_and_reproducibility(tmp_path: Path) -> None:
    db = tmp_path / "olist.duckdb"
    uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    client = mlflow.MlflowClient(tracking_uri=uri, registry_uri=uri)
    name = settings.MLFLOW_MODEL_NAME

    assert _job("demo_prepare_job").execute_in_process().success
    n_raw = _count(db, "select count(*) from raw.orders")
    assert {"raw", "staging", "marts"} <= _schemas(db)
    assert len(client.search_model_versions(f"name='{name}'")) == 1
    assert list((tmp_path / "ml").glob("*.parquet"))

    # derived: raw остаётся, производное и MLflow — нет
    assert _job("clean_derived_job").execute_in_process().success
    assert "raw" in _schemas(db) and not {"staging", "marts", "ml"} & _schemas(db)
    assert client.search_model_versions(f"name='{name}'") == []
    assert not list((tmp_path / "ml").glob("*.parquet"))

    # raw
    assert _job("clean_raw_job").execute_in_process().success
    assert "raw" not in _schemas(db)

    # воспроизводимость: prepare → clean_all → prepare → те же counts и версия 1
    assert _job("demo_prepare_job").execute_in_process().success
    assert _job("clean_all_job").execute_in_process().success
    assert not {"raw", "staging", "marts", "ml"} & _schemas(db)
    assert _job("demo_prepare_job").execute_in_process().success
    assert _count(db, "select count(*) from raw.orders") == n_raw
    versions = client.search_model_versions(f"name='{name}'")
    assert [int(v.version) for v in versions] == [1]
