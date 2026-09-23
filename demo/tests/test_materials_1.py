"""Смоук материалов демо 1: каждая версия defs/ml.py материализуется in-process на выгруженной витрине.

Запуск: `just demo-1-test` (нужен demo/data/mart_order_features.parquet → `just demo-1-prepare`).
`olist_ml.features` в проекте демо = `olist_ml.ml.features` основного репо — подменяем алиасом модуля.
"""

import importlib.util
import sys

import dagster as dg
import pytest
from steps import M1, MART_PARQUET, ML_VERSIONS

from olist_ml.ml import features

sys.modules["olist_ml.features"] = features

pytestmark = pytest.mark.skipif(not MART_PARQUET.exists(), reason="нет parquet — just demo-1-prepare")


def _defs(name: str) -> list:
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), M1 / name)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return [v for v in vars(mod).values() if isinstance(v, dg.AssetsDefinition | dg.AssetChecksDefinition)]


@pytest.mark.parametrize("name", ML_VERSIONS)
def test_version_materializes(name, monkeypatch):
    monkeypatch.setenv("MART_PATH", str(MART_PARQUET))
    res = dg.materialize(_defs(name))
    assert res.success
    assert 0.5 < res.output_for_node("model_evaluation")["roc_auc"] < 1.0


def test_gate_fails_on_high_threshold(monkeypatch):
    monkeypatch.setenv("MART_PATH", str(MART_PARQUET))
    res = dg.materialize(
        _defs("ml_v3_checks.py"),
        run_config={"ops": {"model_evaluation_quality_gate": {"config": {"min_roc_auc": 0.99}}}},
        raise_on_error=False,
    )
    assert any(e.check_name == "quality_gate" and not e.passed for e in res.get_asset_check_evaluations())
