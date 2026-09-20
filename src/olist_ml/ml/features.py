"""Feature contract и чистые функции обучения (ADR-06, ADR-14). Никакого Dagster и I/O.

Признаки — 10 колонок витрины, известных в момент оформления заказа (docs/contracts/features.md).
Модель — один sklearn Pipeline: preprocessing (impute + scale / one-hot) внутри, fit только на train.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

TARGET = "is_late_delivery"
NUMERIC: tuple[str, ...] = (
    "items_cnt",
    "order_value",
    "freight_share",
    "max_installments",
    "customer_seller_distance_km",
    "estimated_delivery_span_days",
    "purchase_month",
    "total_weight_g",
)
CATEGORICAL: tuple[str, ...] = ("customer_state", "main_payment_type")
FEATURES: tuple[str, ...] = NUMERIC + CATEGORICAL
KEY = "order_id"

_HASH_SPACE = 10_000


def _bucket(order_id: str, seed: int) -> int:
    return int(hashlib.md5(f"{order_id}|split|{seed}".encode()).hexdigest()[:8], 16) % _HASH_SPACE


def split_by_hash(
    df: pd.DataFrame, test_frac: float, seed: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Детерминированный сплит по md5(order_id): не зависит от порядка строк; вложен по test_frac."""
    if not (0.0 < test_frac < 1.0):
        raise ValueError(f"test_frac must be in (0, 1), got {test_frac!r}")
    ordered = df.sort_values(KEY, kind="mergesort").reset_index(drop=True)
    buckets = ordered[KEY].astype(str).map(lambda v: _bucket(v, seed))
    is_test = buckets < int(test_frac * _HASH_SPACE)
    return ordered[~is_test].reset_index(drop=True), ordered[is_test].reset_index(drop=True)


def build_pipeline(
    numeric: tuple[str, ...] = NUMERIC,
    categorical: tuple[str, ...] = CATEGORICAL,
    random_state: int = 0,
) -> Pipeline:
    pre = ColumnTransformer(
        [
            (
                "num",
                Pipeline(
                    [("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]
                ),
                list(numeric),
            ),
            ("cat", OneHotEncoder(handle_unknown="ignore"), list(categorical)),
        ]
    )
    model = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=random_state)
    return Pipeline([("pre", pre), ("clf", model)])


def evaluate(pipe: Pipeline, x: pd.DataFrame, y: pd.Series, threshold: float) -> dict[str, float]:
    proba = pipe.predict_proba(x)[:, 1]
    return {
        "roc_auc": float(roc_auc_score(y, proba)),
        "pr_auc": float(average_precision_score(y, proba)),
        "accuracy": float(accuracy_score(y, (proba >= threshold).astype(int))),
        "positive_rate": float(np.mean(y)),
        "n_rows": int(len(y)),
    }


def dataset_fingerprint(df: pd.DataFrame, params: dict[str, Any]) -> str:
    """md5 отсортированных order_id + параметров: не зависит от порядка строк, меняется с params."""
    ids = "|".join(sorted(df[KEY].astype(str)))
    payload = ids + "#" + json.dumps(params, sort_keys=True, default=str)
    return hashlib.md5(payload.encode()).hexdigest()


def gate(value: float, threshold: float) -> bool:
    return value >= threshold
