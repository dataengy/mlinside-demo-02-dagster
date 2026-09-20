"""train_job на временных DuckDB + MLflow: контур A целиком, gate, идемпотентность регистрации, ADR-04a (A).

Сценарии: seed → feature-mart → train_job (версия 1 без алиаса) → повтор (reused_existing) →
gate с порогом 0.99 (quality_gate красный, model_registered не запущен) → сломанный контракт витрины
(blocking dbt-check в том же run → ML-ассеты не материализованы).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import dagster as dg
import pytest

from olist_ml.settings import PROJECT_ROOT, settings

pytestmark = pytest.mark.integration

SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"
ML_KEYS = {
    dg.AssetKey(k) for k in ("training_dataset", "model", "model_evaluation", "model_registered")
}


def _run_py(*args: str) -> None:
    subprocess.run([sys.executable, *args], check=True, timeout=300)


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


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "olist.duckdb"))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow.db'}")
    monkeypatch.setenv("MLFLOW_ARTIFACTS_DIR", str(tmp_path / "mlruns"))
    monkeypatch.setenv("ML_DATA_DIR", str(tmp_path / "ml"))
    (tmp_path / "home").mkdir()
    yield tmp_path
    _run_py(str(PROJECT_ROOT / "scripts" / "demo_patch.py"), "fix")
    _parse()


def _job(name: str):
    from olist_ml.definitions import defs

    return defs().resolve_job_def(name)


def _mats(r: dg.ExecuteInProcessResult) -> set[dg.AssetKey]:
    return {e.asset_key for e in r.get_asset_materialization_events()}


def _checks(r: dg.ExecuteInProcessResult) -> dict[str, bool]:
    return {e.check_name: e.passed for e in r.get_asset_check_evaluations()}


def _registered_meta(r: dg.ExecuteInProcessResult) -> dict:
    ev = [
        e
        for e in r.get_asset_materialization_events()
        if e.asset_key == dg.AssetKey("model_registered")
    ]
    return {k: v.value for k, v in ev[0].materialization.metadata.items()}


def _prepare() -> None:
    assert _job("demo_prepare_job").execute_in_process().success
    assert _job("feature_mart_job").execute_in_process().success


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_train_job_registers_version_without_alias_and_is_idempotent(env: Path) -> None:
    import mlflow

    _prepare()
    r1 = _job("train_job").execute_in_process()
    assert r1.success
    assert _mats(r1) >= ML_KEYS
    checks = _checks(r1)
    assert checks["quality_gate"] is True
    assert any("unique" in n and "order_id" in n for n in checks), (
        "dbt-checks витрины идут в том же run"
    )
    meta = _registered_meta(r1)
    assert meta["model_version"] == 1 and meta["reused_existing"] is False

    client = mlflow.MlflowClient(tracking_uri=f"sqlite:///{env / 'mlflow.db'}")
    versions = client.search_model_versions(f"name='{settings.MLFLOW_MODEL_NAME}'")
    assert len(versions) == 1 and not versions[0].aliases

    r2 = _job("train_job").execute_in_process()
    assert r2.success
    meta2 = _registered_meta(r2)
    assert meta2["model_version"] == 1 and meta2["reused_existing"] is True
    assert len(client.search_model_versions(f"name='{settings.MLFLOW_MODEL_NAME}'")) == 1


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_gate_failure_blocks_registration(env: Path) -> None:
    _prepare()
    r = _job("train_job").execute_in_process(
        run_config={"ops": {"model_evaluation_quality_gate": {"config": {"min_roc_auc": 0.99}}}},
        raise_on_error=False,
    )
    checks = _checks(r)
    assert checks["quality_gate"] is False
    mats = _mats(r)
    assert dg.AssetKey("model_evaluation") in mats
    assert dg.AssetKey("model_registered") not in mats


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_broken_mart_contract_skips_ml_branch(env: Path) -> None:
    _prepare()
    _run_py(str(PROJECT_ROOT / "scripts" / "demo_patch.py"), "break")
    _parse()
    assert _job("feature_mart_job").execute_in_process().success  # модель зелёная, дубли в витрине
    r = _job("train_job").execute_in_process(raise_on_error=False)
    checks = _checks(r)
    assert any("unique" in n and "order_id" in n and not ok for n, ok in checks.items()), checks
    assert not (ML_KEYS & _mats(r)), (
        "ADR-04a (A): blocking check витрины должен остановить ML-ветку"
    )
