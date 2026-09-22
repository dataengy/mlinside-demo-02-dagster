"""sample_connected: связность по ключам, детерминизм, независимость от порядка строк."""

from __future__ import annotations

import pandas as pd
import pytest

from olist_ml.sampling import RAW_TABLES, frac_for_n_orders, sample_connected

pytestmark = pytest.mark.unit


def _synthetic(n_orders: int = 40) -> dict[str, pd.DataFrame]:
    oids = [f"o{i}" for i in range(n_orders)]
    orders = pd.DataFrame({"order_id": oids, "customer_id": [f"c{i}" for i in range(n_orders)]})
    items = pd.DataFrame(
        {
            "order_id": [o for o in oids for _ in range(2)],
            "order_item_id": [1, 2] * n_orders,
            "product_id": [f"p{i % 7}" for i in range(2 * n_orders)],
            "seller_id": [f"s{i % 5}" for i in range(2 * n_orders)],
        }
    )
    customers = pd.DataFrame(
        {
            "customer_id": [f"c{i}" for i in range(n_orders)],
            "customer_zip_code_prefix": [f"{i % 9:05d}" for i in range(n_orders)],
        }
    )
    sellers = pd.DataFrame(
        {
            "seller_id": [f"s{i}" for i in range(5)],
            "seller_zip_code_prefix": [f"9{i:04d}" for i in range(5)],
        }
    )
    products = pd.DataFrame({"product_id": [f"p{i}" for i in range(7)]})
    geo = pd.DataFrame(
        {
            "geolocation_zip_code_prefix": [f"{i % 9:05d}" for i in range(90)] + ["77777"] * 3,
            "geolocation_lat": [float(i) for i in range(93)],
            "geolocation_lng": 0.0,
        }
    )
    return {
        "orders": orders,
        "order_items": items,
        "order_payments": pd.DataFrame({"order_id": oids, "payment_sequential": 1}),
        "order_reviews": pd.DataFrame(
            {"order_id": oids[:10], "review_id": [f"r{i}" for i in range(10)]}
        ),
        "customers": customers,
        "sellers": sellers,
        "products": products,
        "geolocation": geo,
    }


def test_referential_integrity() -> None:
    s = sample_connected(_synthetic(), 0.5, seed=1)
    oids = set(s["orders"]["order_id"])
    assert 0 < len(oids) < 40
    for child in ("order_items", "order_payments", "order_reviews"):
        assert set(s[child]["order_id"]) <= oids
    assert set(s["orders"]["customer_id"]) == set(s["customers"]["customer_id"])
    assert set(s["order_items"]["product_id"]) <= set(s["products"]["product_id"])
    assert set(s["order_items"]["seller_id"]) <= set(s["sellers"]["seller_id"])
    assert "77777" not in set(s["geolocation"]["geolocation_zip_code_prefix"])
    assert set(s) == set(RAW_TABLES)


def test_deterministic_and_order_independent() -> None:
    t = _synthetic()
    a = sample_connected(t, 0.3, seed=7)
    shuffled = {k: v.sample(frac=1.0, random_state=3) for k, v in t.items()}
    b = sample_connected(shuffled, 0.3, seed=7)
    assert set(a["orders"]["order_id"]) == set(b["orders"]["order_id"])
    assert set(a["orders"]["order_id"]) != set(
        sample_connected(t, 0.3, seed=8)["orders"]["order_id"]
    )


def test_full_and_invalid_frac() -> None:
    t = _synthetic()
    assert len(sample_connected(t, 1.0, seed=1)["orders"]) == 40
    with pytest.raises(ValueError):
        sample_connected(t, 0.0, seed=1)


def test_geo_cap() -> None:
    s = sample_connected(_synthetic(), 1.0, seed=1, geo_max_rows=20)
    assert len(s["geolocation"]) <= 20


def test_frac_for_n_orders() -> None:
    assert frac_for_n_orders(100, 500) == 1.0
    assert frac_for_n_orders(1000, 100) == pytest.approx(0.1)
