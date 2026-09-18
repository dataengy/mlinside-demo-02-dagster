"""Тесты sample_connected() на маленьких синтетических таблицах.

Никакого I/O и никакого Kaggle — только in-memory DataFrame'ы, чтобы тест был
быстрым и годился для уровня unit (см. PROMPT.md, таблица «Тесты», строка
unit: «сэмплирование (связность по order_id)»).

Запуск (из корня LANE, PYTHONPATH=LANE):
    PYTHONPATH=. uv run --python 3.12 --with pandas --with pytest pytest test_sampling.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from sampling import sample_connected


def _synthetic_tables(n_orders: int = 30) -> dict[str, pd.DataFrame]:
    """Маленький, но полносвязный синтетический аналог Olist.

    2 позиции на заказ, product_id/seller_id пересекаются между заказами
    (как в реальных данных — товар продаётся многократно), plus несколько
    geolocation-строк с zip-префиксом, который никто не референсит — чтобы
    проверить, что они действительно отфильтровываются.
    """
    order_ids = [f"order_{i}" for i in range(n_orders)]
    customer_ids = [f"cust_{i}" for i in range(n_orders)]
    orders = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "order_status": ["delivered"] * n_orders,
        }
    )

    items_rows = []
    for i, oid in enumerate(order_ids):
        for j in range(2):
            items_rows.append(
                {
                    "order_id": oid,
                    "order_item_id": str(j + 1),
                    "product_id": f"prod_{(i + j) % max(1, n_orders // 2)}",
                    "seller_id": f"seller_{i % 5}",
                }
            )
    order_items = pd.DataFrame(items_rows)

    order_payments = pd.DataFrame(
        {
            "order_id": order_ids,
            "payment_type": ["credit_card"] * n_orders,
            "payment_value": [100.0] * n_orders,
        }
    )

    order_reviews = pd.DataFrame(
        {
            "review_id": [f"review_{i}" for i in range(n_orders)],
            "order_id": order_ids,
            "review_score": [5] * n_orders,
        }
    )

    customers = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "customer_zip_code_prefix": [f"{1000 + i}" for i in range(n_orders)],
        }
    )

    product_ids = sorted(order_items["product_id"].unique())
    products = pd.DataFrame(
        {
            "product_id": product_ids,
            "product_category_name": ["cat_a"] * len(product_ids),
        }
    )

    seller_ids = sorted(order_items["seller_id"].unique())
    sellers = pd.DataFrame(
        {
            "seller_id": seller_ids,
            "seller_zip_code_prefix": [f"{2000 + i}" for i in range(len(seller_ids))],
        }
    )

    zips = set(customers["customer_zip_code_prefix"]) | set(sellers["seller_zip_code_prefix"])
    geo_rows = [
        {"geolocation_zip_code_prefix": z, "geolocation_lat": float(k), "geolocation_lng": 0.0}
        for z in zips
        for k in range(3)
    ]
    # zip, на который никто не ссылается — должен быть отрезан
    geo_rows.append(
        {"geolocation_zip_code_prefix": "9999999", "geolocation_lat": 1.0, "geolocation_lng": 1.0}
    )
    geolocation = pd.DataFrame(geo_rows)

    product_category_name_translation = pd.DataFrame(
        {
            "product_category_name": ["cat_a", "cat_b"],
            "product_category_name_english": ["category_a", "category_b"],
        }
    )

    return {
        "orders": orders,
        "order_items": order_items,
        "order_payments": order_payments,
        "order_reviews": order_reviews,
        "customers": customers,
        "products": products,
        "sellers": sellers,
        "geolocation": geolocation,
        "product_category_name_translation": product_category_name_translation,
    }


def test_connectivity_items_payments_reviews_reference_selected_orders():
    tables = _synthetic_tables(n_orders=30)
    sampled = sample_connected(tables, sample_frac=0.4, seed=1)
    order_ids = set(sampled["orders"]["order_id"])
    assert len(order_ids) > 0

    for name in ("order_items", "order_payments", "order_reviews"):
        refs = set(sampled[name]["order_id"])
        assert refs <= order_ids, f"{name} ссылается на заказы вне сэмпла"
        # в синтетической фикстуре у каждого заказа есть ровно одна связанная
        # строка/группа строк в каждой child-таблице -> после фильтра по
        # выбранным order_id должны остаться ВСЕ они, без случайных потерь
        assert refs == order_ids, f"{name} потерял часть строк для отобранных заказов"


def test_customers_products_sellers_present_for_selected_orders():
    tables = _synthetic_tables(n_orders=30)
    sampled = sample_connected(tables, sample_frac=0.5, seed=7)

    expected_customers = set(sampled["orders"]["customer_id"])
    assert set(sampled["customers"]["customer_id"]) == expected_customers

    expected_products = set(sampled["order_items"]["product_id"])
    assert set(sampled["products"]["product_id"]) == expected_products

    expected_sellers = set(sampled["order_items"]["seller_id"])
    assert set(sampled["sellers"]["seller_id"]) == expected_sellers


def test_geolocation_restricted_to_referenced_zip_prefixes():
    tables = _synthetic_tables(n_orders=30)
    sampled = sample_connected(tables, sample_frac=0.5, seed=3)

    expected_zips = set(sampled["customers"]["customer_zip_code_prefix"]) | set(
        sampled["sellers"]["seller_zip_code_prefix"]
    )
    actual_zips = set(sampled["geolocation"]["geolocation_zip_code_prefix"])
    assert actual_zips == expected_zips
    assert "9999999" not in actual_zips


def test_frac_one_keeps_everything():
    """frac=1.0 -> все order-таблицы без изменений.

    geolocation — особый случай: она никогда не «сэмплируется по frac» сама
    по себе, а всегда фильтруется по zip-префиксам, реально встречающимся у
    customers/sellers (см. docstring sample_connected). Поэтому даже при
    frac=1.0 заведомо ничей zip-префикс из фикстуры ("9999999") обязан быть
    отрезан — иначе frac=1.0 незаметно отключал бы фильтрацию геолокации,
    которая в проде вырезает часть таблицы с ~1 млн строк (см. риски в
    REPORT.md).
    """
    tables = _synthetic_tables(n_orders=15)
    sampled = sample_connected(tables, sample_frac=1.0, seed=42)

    for name, df in tables.items():
        if name == "geolocation":
            continue
        assert len(sampled[name]) == len(df), f"{name} потерял строки при frac=1.0"
    assert set(sampled["orders"]["order_id"]) == set(tables["orders"]["order_id"])

    referenced_zips = set(sampled["customers"]["customer_zip_code_prefix"]) | set(
        sampled["sellers"]["seller_zip_code_prefix"]
    )
    expected_geo_len = tables["geolocation"]["geolocation_zip_code_prefix"].isin(referenced_zips).sum()
    assert len(sampled["geolocation"]) == expected_geo_len
    assert "9999999" not in set(sampled["geolocation"]["geolocation_zip_code_prefix"])


def test_deterministic_same_seed_same_selection():
    tables = _synthetic_tables(n_orders=50)
    first = sample_connected(tables, sample_frac=0.3, seed=123)
    second = sample_connected(tables, sample_frac=0.3, seed=123)

    assert set(first["orders"]["order_id"]) == set(second["orders"]["order_id"])
    for name in first:
        assert len(first[name]) == len(second[name])


def test_different_seed_can_change_selection():
    tables = _synthetic_tables(n_orders=200)
    a = sample_connected(tables, sample_frac=0.3, seed=1)
    b = sample_connected(tables, sample_frac=0.3, seed=2)
    assert set(a["orders"]["order_id"]) != set(b["orders"]["order_id"])


def test_invalid_frac_raises():
    tables = _synthetic_tables(n_orders=5)
    with pytest.raises(ValueError):
        sample_connected(tables, sample_frac=0.0, seed=1)
    with pytest.raises(ValueError):
        sample_connected(tables, sample_frac=1.5, seed=1)


def test_missing_orders_key_raises():
    with pytest.raises(KeyError):
        sample_connected({"customers": pd.DataFrame({"customer_id": ["a"]})}, sample_frac=0.5, seed=1)
