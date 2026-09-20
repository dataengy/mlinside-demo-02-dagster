"""Общие фикстуры. Тесты не ходят в сеть и не требуют поднятого UI.

manifest.json нужен компонентам dbt (prepare_if_dev выключен, ADR-04b): если его нет — собираем
один раз на сессию через `dbt parse` (офлайн; пакеты dbt должны быть установлены `just install`).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"
MANIFEST = DBT_DIR / "target" / "manifest.json"

os.environ.setdefault("DBT_VERSION_CHECK", "false")
os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
os.environ.setdefault("DBT_INDIRECT_SELECTION", "cautious")


@pytest.fixture(scope="session", autouse=True)
def dbt_manifest() -> Path | None:
    if not DBT_DIR.is_dir():
        return None
    if not MANIFEST.is_file():
        dbt = Path(sys.executable).parent / "dbt"
        subprocess.run(
            [
                str(dbt),
                "parse",
                "--quiet",
                "--no-version-check",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
            ],
            check=True,
            timeout=300,
        )
    return MANIFEST


@pytest.fixture(autouse=True)
def _isolate_stores(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Интеграционные тесты никогда не пишут в реальные data/olist.duckdb, data/mlflow.db, data/ml.

    demo_prepare_job обучает baseline (ADR-07a A) → без изоляции тест засорял бы реальный реестр MLflow
    и глобальный registry URI процесса. Тесты могут переопределить пути своими monkeypatch'ами.
    """
    if not any(part in str(request.node.fspath) for part in ("integration", "e2e")):
        return
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "olist.duckdb"))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow.db'}")
    monkeypatch.setenv("MLFLOW_ARTIFACTS_DIR", str(tmp_path / "mlruns"))
    monkeypatch.setenv("ML_DATA_DIR", str(tmp_path / "ml"))
    (tmp_path / "home").mkdir(exist_ok=True)
