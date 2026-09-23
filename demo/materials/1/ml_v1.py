"""Демо 1, шаг 2 — четыре ассета: mart_order_features → training_dataset → model → model_evaluation.

Кладётся в <project>/src/olist_ml/defs/ml.py; рядом — src/olist_ml/features.py (копия из основного репо).
Витрина читается из parquet (во 2-м демо её место займёт dbt-модель с тем же ключом).
"""

import os
from pathlib import Path

import dagster as dg
import pandas as pd

from olist_ml import features as F

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MART_PATH = Path(os.getenv("MART_PATH", PROJECT_ROOT / "data" / "mart_order_features.parquet"))


@dg.asset(group_name="ml", kinds={"parquet"})
def mart_order_features() -> pd.DataFrame:
    """Витрина признаков (пока — готовый parquet от DE-команды)."""
    return pd.read_parquet(MART_PATH)


@dg.asset(group_name="ml", kinds={"pandas"})
def training_dataset(mart_order_features: pd.DataFrame) -> dict:
    """Строки с известным target, детерминированный сплит train/holdout по md5(order_id)."""
    df = mart_order_features[mart_order_features[F.TARGET].notna()]
    train, holdout = F.split_by_hash(df, test_frac=0.2, seed=42)
    return {"train": train, "holdout": holdout}


@dg.asset(group_name="ml", kinds={"sklearn"}, code_version="v1")
def model(training_dataset: dict):
    """sklearn Pipeline (preprocessing + LogisticRegression), fit только на train."""
    train = training_dataset["train"]
    pipe = F.build_pipeline(random_state=42)
    pipe.fit(train[list(F.FEATURES)], train[F.TARGET].astype(int))
    return pipe


@dg.asset(group_name="ml", kinds={"sklearn"})
def model_evaluation(model, training_dataset: dict) -> dict:
    """Метрики на holdout."""
    h = training_dataset["holdout"]
    return F.evaluate(model, h[list(F.FEATURES)], h[F.TARGET].astype(int), threshold=0.5)
