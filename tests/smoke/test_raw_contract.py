"""Контракт raw (ADR-14, docs/contracts/raw.md): sources.yml ↔ shipped snapshot без БД.

Каждая source-таблица имеет seed-файл dbt/seeds/raw/<table>.csv; все колонки из sources.yml есть в
заголовке CSV; ключи ассетов — [raw, <table>]; служебных колонок загрузчика нет.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
import yaml

from olist_ml.sampling import RAW_TABLES
from olist_ml.settings import settings

pytestmark = pytest.mark.smoke

SOURCES_YML = settings.DBT_PROJECT_DIR / "models" / "sources" / "sources.yml"
SEEDS_DIR = settings.DBT_PROJECT_DIR / "seeds" / "raw"


def _sources() -> list[dict]:
    doc = yaml.safe_load(SOURCES_YML.read_text())
    (src,) = doc["sources"]
    return src["tables"]


def _header(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


@pytest.mark.skipif(not SOURCES_YML.is_file(), reason="dbt/ не синхронизирован (just dbt-sync)")
def test_every_source_has_seed_and_asset_key() -> None:
    tables = _sources()
    assert {t["name"] for t in tables} == set(RAW_TABLES)
    for t in tables:
        assert t["meta"]["dagster"]["asset_key"] == ["raw", t["name"]], t["name"]
        assert "external_location" not in t.get("meta", {}), t["name"]
        assert (SEEDS_DIR / f"{t['name']}.csv").is_file(), f"нет seed для {t['name']}"


@pytest.mark.skipif(not SEEDS_DIR.is_dir(), reason="snapshot ещё не сгенерирован")
def test_source_columns_exist_in_snapshot() -> None:
    for t in _sources():
        header = _header(SEEDS_DIR / f"{t['name']}.csv")
        assert not any(c.startswith("_dlt") for c in header), t["name"]
        missing = [c["name"] for c in t.get("columns", []) if c["name"] not in header]
        assert not missing, f"{t['name']}: колонок из sources.yml нет в snapshot: {missing}"
