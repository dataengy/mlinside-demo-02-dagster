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
