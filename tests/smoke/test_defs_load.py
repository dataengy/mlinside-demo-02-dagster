"""Smoke: definitions импортируются и `dg check defs` проходит (без UI, без сети)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

ROOT = Path(__file__).resolve().parents[2]


def test_definitions_import() -> None:
    from olist_ml.definitions import defs

    assert defs() is not None


def test_settings_import_has_no_side_effects() -> None:
    from olist_ml.settings import settings

    assert settings.DAGSTER_PORT > 0


def test_dg_check_defs(tmp_path: Path) -> None:
    dg = Path(sys.executable).parent / "dg"  # бинарь из того же venv, что и pytest
    result = subprocess.run(
        [str(dg), "check", "defs"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        env={**os.environ, "DAGSTER_HOME": str(tmp_path), "MLFLOW_DISABLE_AGENT_HINT": "1"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
