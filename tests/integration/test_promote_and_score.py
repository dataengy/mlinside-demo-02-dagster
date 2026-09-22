"""Контур B: baseline из demo_prepare (без алиаса) → score без champion падает понятно → promote →
predictions с model_version → повтор без дублей → новый кандидат не меняет champion → demote."""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import duckdb
import mlflow
import pytest

from olist_ml.settings import settings

pytestmark = pytest.mark.integration

SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"
NAME, ALIAS = settings.MLFLOW_MODEL_NAME, settings.MLFLOW_MODEL_ALIAS


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "olist.duckdb"))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow.db'}")
    monkeypatch.setenv("MLFLOW_ARTIFACTS_DIR", str(tmp_path / "mlruns"))
    monkeypatch.setenv("ML_DATA_DIR", str(tmp_path / "ml"))
    (tmp_path / "home").mkdir(exist_ok=True)
    return tmp_path


def _job(name: str):
    from olist_ml.definitions import defs

    return defs().resolve_job_def(name)


def _q(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _meta(r: dg.ExecuteInProcessResult, key: str) -> dict:
    ev = [e for e in r.get_asset_materialization_events() if e.asset_key == dg.AssetKey(key)]
    return {k: v.value for k, v in ev[0].materialization.metadata.items()}


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_promote_then_score_is_idempotent_and_alias_driven(env: Path) -> None:
    db = env / "olist.duckdb"
    uri = f"sqlite:///{env / 'mlflow.db'}"
    client = mlflow.MlflowClient(tracking_uri=uri, registry_uri=uri)

    # demo_prepare: raw → витрина → baseline v1 без алиаса (ADR-07a A)
    assert _job("demo_prepare_job").execute_in_process().success
    versions = client.search_model_versions(f"name='{NAME}'")
    diag = [
        (v.version, v.aliases, client.get_run(v.run_id).data.tags.get("role"), v.run_id[:8])
        for v in versions
    ]
    assert [int(v.version) for v in versions] == [1] and not versions[0].aliases, (
        diag,
        mlflow.get_tracking_uri(),
        mlflow.get_registry_uri(),
    )
    assert client.get_run(versions[0].run_id).data.tags["role"] == "baseline"

    # без champion — понятная остановка
    r = _job("score_job").execute_in_process(raise_on_error=False)
    assert not r.success
    assert any(
        "promote" in (ev.step_failure_data.error.message if ev.step_failure_data.error else "")
        for ev in r.get_step_failure_events()
    )

    # promote → champion = 1 → score
    assert _job("promote_job").execute_in_process().success
    assert int(client.get_model_version_by_alias(NAME, ALIAS).version) == 1
    r1 = _job("score_job").execute_in_process()
    assert r1.success
    m = _meta(r1, "predictions")
    n_unknown = _q(
        db, "select count(*) from marts.mart_order_features where is_late_delivery is null"
    )[0][0]
    assert m["rows"] == n_unknown > 0 and m["model_version"] == 1
    assert _q(db, "select count(*), count(distinct order_id) from ml.predictions")[0] == (
        n_unknown,
        n_unknown,
    )

    # повтор того же batch — без дублей
    assert _job("score_job").execute_in_process().success
    assert _q(db, "select count(*) from ml.predictions")[0][0] == n_unknown

    # новый кандидат (другой сид → v2) champion не трогает; scoring по-прежнему v1
    assert _job("train_job").execute_in_process().success
    assert sorted(int(v.version) for v in client.search_model_versions(f"name='{NAME}'")) == [1, 2]
    assert int(client.get_model_version_by_alias(NAME, ALIAS).version) == 1
    assert _meta(_job("score_job").execute_in_process(), "predictions")["model_version"] == 1

    # promote v2 явно → scoring v2; demote → снова понятная ошибка
    assert (
        _job("promote_job")
        .execute_in_process(run_config={"ops": {"promote_champion": {"config": {"version": 2}}}})
        .success
    )
    assert _meta(_job("score_job").execute_in_process(), "predictions")["model_version"] == 2
    assert _job("demote_job").execute_in_process().success
    assert not _job("score_job").execute_in_process(raise_on_error=False).success
