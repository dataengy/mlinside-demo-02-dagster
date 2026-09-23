"""Демо 1, шаг 4 — то же + metadata: строки, positive rate, ROC AUC видны в UI и копятся по запускам."""

import os
from pathlib import Path

import dagster as dg
import pandas as pd

from olist_ml import features as F

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MART_PATH = Path(os.getenv("MART_PATH", PROJECT_ROOT / "data" / "mart_order_features.parquet"))


@dg.asset(group_name="ml", kinds={"parquet"})
def mart_order_features() -> dg.Output[pd.DataFrame]:
    """Витрина признаков (пока — готовый parquet от DE-команды)."""
    df = pd.read_parquet(MART_PATH)
    return dg.Output(df, metadata={"rows": len(df), "path": str(MART_PATH)})


@dg.asset(group_name="ml", kinds={"pandas"})
def training_dataset(mart_order_features: pd.DataFrame) -> dg.Output[dict]:
    """Строки с известным target, детерминированный сплит train/holdout по md5(order_id)."""
    df = mart_order_features[mart_order_features[F.TARGET].notna()]
    train, holdout = F.split_by_hash(df, test_frac=0.2, seed=42)
    return dg.Output(
        {"train": train, "holdout": holdout},
        metadata={
            "rows_train": len(train),
            "rows_holdout": len(holdout),
            "positive_rate": round(float(df[F.TARGET].astype(int).mean()), 4),
            "features": dg.MetadataValue.json(list(F.FEATURES)),
        },
    )


@dg.asset(group_name="ml", kinds={"sklearn"}, code_version="v1")
def model(training_dataset: dict):
    """sklearn Pipeline (preprocessing + LogisticRegression), fit только на train."""
    train = training_dataset["train"]
    pipe = F.build_pipeline(random_state=42)
    pipe.fit(train[list(F.FEATURES)], train[F.TARGET].astype(int))
    return pipe


@dg.asset(group_name="ml", kinds={"sklearn"})
def model_evaluation(model, training_dataset: dict) -> dg.Output[dict]:
    """Метрики на holdout — в metadata: UI строит график по истории материализаций."""
    h = training_dataset["holdout"]
    m = F.evaluate(model, h[list(F.FEATURES)], h[F.TARGET].astype(int), threshold=0.5)
    return dg.Output(
        m,
        metadata={
            "roc_auc": round(m["roc_auc"], 4),
            "pr_auc": round(m["pr_auc"], 4),
            "accuracy": round(m["accuracy"], 4),
            "rows_holdout": m["n_rows"],
        },
    )
