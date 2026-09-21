"""e2e: весь сюжет демо на shipped snapshot во временных путях — как в DEMO.md, без UI.

clean_all → demo_prepare (raw, витрина, baseline v1) → dq → train (v2) → promote → score (v2) →
demo-break → dq красный → train пропускает ML → demo-fix → clean_all → demo_prepare воспроизводит.
Отдельная стадия CI (`just test-e2e`): дольше и покрывает то же, что интеграционные тесты, но одним потоком.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import dagster as dg
import duckdb
import mlflow
import pytest

from olist_ml.settings import PROJECT_ROOT, settings

pytestmark = pytest.mark.e2e

SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"
NAME, ALIAS = settings.MLFLOW_MODEL_NAME, settings.MLFLOW_MODEL_ALIAS


def _job(name: str):
    from olist_ml.definitions import defs

    return defs().resolve_job_def(name)


def _patch(mode: str) -> None:
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "demo_patch.py"), mode],
        check=True,
        timeout=60,
    )
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


def _count(db: Path, sql: str) -> int:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def _mats(r: dg.ExecuteInProcessResult) -> set[str]:
    return {e.asset_key.to_user_string() for e in r.get_asset_materialization_events()}


def _meta(r: dg.ExecuteInProcessResult, key: str) -> dict:
    ev = [e for e in r.get_asset_materialization_events() if e.asset_key == dg.AssetKey(key)]
    return {k: v.value for k, v in ev[0].materialization.metadata.items()}


@pytest.fixture
def stores(tmp_path: Path):
    try:
        yield tmp_path
    finally:
        _patch("fix")


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_full_demo_flow(stores: Path) -> None:
    db = stores / "olist.duckdb"
    uri = f"sqlite:///{stores / 'mlflow.db'}"
    client = mlflow.MlflowClient(tracking_uri=uri, registry_uri=uri)

    assert _job("clean_all_job").execute_in_process().success  # на пустом состоянии — ок
    assert _job("demo_prepare_job").execute_in_process().success
    n_orders = _count(db, "select count(*) from raw.orders")
    n_mart = _count(db, "select count(*) from marts.mart_order_features")
    assert n_mart == n_orders > 0
    assert [int(v.version) for v in client.search_model_versions(f"name='{NAME}'")] == [1]

    assert _job("dq_job").execute_in_process().success
    assert _job("train_job").execute_in_process().success
    assert sorted(int(v.version) for v in client.search_model_versions(f"name='{NAME}'")) == [1, 2]

    assert _job("promote_job").execute_in_process().success  # → последняя (v2)
    r = _job("score_job").execute_in_process()
    assert r.success and _meta(r, "predictions")["model_version"] == 2
    n_pred = _count(db, "select count(*) from ml.predictions")
    assert n_pred == _count(
        db, "select count(*) from marts.mart_order_features where is_late_delivery is null"
    )

    _patch("break")
    assert _job("feature_mart_job").execute_in_process().success
    assert not _job("dq_job").execute_in_process(raise_on_error=False).success
    r = _job("train_job").execute_in_process(raise_on_error=False)
    assert not _mats(r) & {"training_dataset", "model", "model_registered"}
    _patch("fix")
    assert _job("feature_mart_job").execute_in_process().success
    assert _job("dq_job").execute_in_process().success

    assert _job("clean_all_job").execute_in_process().success
    assert _job("demo_prepare_job").execute_in_process().success
    assert _count(db, "select count(*) from raw.orders") == n_orders
    assert [int(v.version) for v in client.search_model_versions(f"name='{NAME}'")] == [1]
