"""Чистые функции ML: сплит (детерминизм, доля, вложенность, независимость от порядка), fingerprint, gate."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from olist_ml.ml import features as F

pytestmark = pytest.mark.unit


def _df(n: int = 2000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            F.KEY: [f"{i:032x}" for i in range(n)],
            F.TARGET: rng.integers(0, 2, n),
            "items_cnt": rng.integers(1, 5, n),
            "order_value": rng.uniform(10, 500, n),
            "freight_share": rng.uniform(0, 1, n),
            "max_installments": rng.integers(1, 12, n),
            "customer_seller_distance_km": np.where(
                rng.uniform(size=n) < 0.1, np.nan, rng.uniform(1, 3000, n)
            ),
            "estimated_delivery_span_days": rng.integers(5, 40, n),
            "purchase_month": rng.integers(1, 13, n),
            "total_weight_g": rng.uniform(50, 30000, n),
            "customer_state": rng.choice(["SP", "RJ", "MG", "BA"], n),
            "main_payment_type": rng.choice(["credit_card", "boleto", "voucher"], n),
        }
    )


def test_split_is_deterministic_and_order_independent() -> None:
    df = _df()
    a_train, a_test = F.split_by_hash(df, 0.2, seed=42)
    b_train, b_test = F.split_by_hash(df.sample(frac=1.0, random_state=9), 0.2, seed=42)
    assert set(a_test[F.KEY]) == set(b_test[F.KEY])
    assert set(a_train[F.KEY]) | set(a_test[F.KEY]) == set(df[F.KEY])
    assert not set(a_train[F.KEY]) & set(a_test[F.KEY])
    assert abs(len(a_test) / len(df) - 0.2) < 0.03


def test_split_nested_and_seed_sensitive() -> None:
    df = _df()
    _, small = F.split_by_hash(df, 0.1, seed=42)
    _, big = F.split_by_hash(df, 0.2, seed=42)
    assert set(small[F.KEY]) <= set(big[F.KEY])
    _, other = F.split_by_hash(df, 0.2, seed=43)
    assert set(other[F.KEY]) != set(big[F.KEY])
    with pytest.raises(ValueError):
        F.split_by_hash(df, 1.0, seed=1)


def test_pipeline_trains_and_evaluates_with_nans_and_unknown_categories() -> None:
    df = _df()
    train, test = F.split_by_hash(df, 0.2, seed=1)
    pipe = F.build_pipeline(random_state=1).fit(train[list(F.FEATURES)], train[F.TARGET])
    test = test.copy()
    test.loc[test.index[:5], "customer_state"] = "ZZ"  # категория, которой не было в train
    m = F.evaluate(pipe, test[list(F.FEATURES)], test[F.TARGET], threshold=0.5)
    assert set(m) == {"roc_auc", "pr_auc", "accuracy", "positive_rate", "n_rows"}
    assert 0.0 <= m["roc_auc"] <= 1.0 and m["n_rows"] == len(test)


def test_fingerprint_order_independent_and_param_sensitive() -> None:
    df = _df(200)
    p = {"test_frac": 0.2, "random_state": 42}
    assert F.dataset_fingerprint(df, p) == F.dataset_fingerprint(df.iloc[::-1], p)
    assert F.dataset_fingerprint(df, p) != F.dataset_fingerprint(df, {**p, "random_state": 7})
    assert F.dataset_fingerprint(df, p) != F.dataset_fingerprint(df.iloc[:-1], p)


def test_gate_is_inclusive() -> None:
    assert F.gate(0.70, 0.70) and not F.gate(0.699, 0.70)


def test_feature_contract_has_no_leakage_columns() -> None:
    forbidden = {
        "delivery_days",
        "delivery_delay_days",
        "review_score",
        "order_status",
        "is_canceled",
    }
    assert not forbidden & set(F.FEATURES)
    assert 8 <= len(F.FEATURES) <= 10
