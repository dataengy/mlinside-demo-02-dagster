"""feature_mart_job / dq_job раздельно на временной DuckDB; идемпотентность; негативный сценарий.

Порядок: seed → feature_mart_job (материализации, без checks) → dq_job (checks, без материализаций)
→ повтор → те же counts → `break` → модель зелёная, check unique(order_id) красный → `fix`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from olist_ml.settings import PROJECT_ROOT, settings

pytestmark = pytest.mark.integration

MART_KEY = dg.AssetKey(["mart_order_features"])
SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"


def _parse() -> None:
    dbt = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [
            str(dbt),
            "parse",
            "--quiet",
            "--no-version-check",
            "--project-dir",
            str(settings.DBT_PROJECT_DIR),
            "--profiles-dir",
            str(settings.DBT_PROFILES_DIR),
        ],
        check=True,
        timeout=300,
    )


def _patch(mode: str) -> None:
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "demo_patch.py"), mode],
        check=True,
        timeout=60,
    )
    _parse()


def _count(db: Path, sql: str) -> int:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def _materialized_keys(result: dg.ExecuteInProcessResult) -> set[dg.AssetKey]:
    return {e.asset_key for e in result.get_asset_materialization_events()}


def _check_results(result: dg.ExecuteInProcessResult) -> dict[str, bool]:
    return {e.check_name: e.passed for e in result.get_asset_check_evaluations()}


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db = tmp_path / "olist.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir(exist_ok=True)
    yield db
    _patch("fix")  # даже если тест упал посреди негативного сценария


def _job(name: str):
    from olist_ml.definitions import defs

    return defs().resolve_job_def(name)


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_feature_mart_and_dq_are_separate_and_idempotent(env: Path) -> None:
    assert _job("demo_prepare_job").execute_in_process().success

    r1 = _job("feature_mart_job").execute_in_process()
    assert r1.success
    assert MART_KEY in _materialized_keys(r1)
    assert not _materialized_keys(r1) & {dg.AssetKey(["raw", "orders"])}
    assert _check_results(r1) == {}, "feature_mart_job не должен запускать dbt-тесты"
    n_mart = _count(env, "select count(*) from marts.mart_order_features")
    assert n_mart == _count(env, "select count(*) from raw.orders")
    assert (
        _count(env, "select count(*) from marts.mart_order_features where is_late_delivery is null")
        > 0
    )

    r2 = _job("dq_job").execute_in_process()
    assert r2.success
    assert _materialized_keys(r2) == set(), "dq_job не должен материализовать модели"
    checks = _check_results(r2)
    assert checks and all(checks.values()), {k: v for k, v in checks.items() if not v}

    r3 = _job("feature_mart_job").execute_in_process()
    assert r3.success
    assert _count(env, "select count(*) from marts.mart_order_features") == n_mart


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_broken_contract_green_model_red_check(env: Path) -> None:
    assert _job("demo_prepare_job").execute_in_process().success
    _patch("break")
    r_model = _job("feature_mart_job").execute_in_process()
    assert r_model.success and MART_KEY in _materialized_keys(r_model)
    assert (
        _count(env, "select count(*) - count(distinct order_id) from marts.mart_order_features") > 0
    )

    r_dq = _job("dq_job").execute_in_process(raise_on_error=False)
    checks = _check_results(r_dq)
    failed = {name for name, ok in checks.items() if not ok}
    assert any("unique" in n and "order_id" in n for n in failed), checks
    assert _materialized_keys(r_dq) == set()
