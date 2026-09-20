"""Детерминированное связное сэмплирование таблиц Olist (ADR-03).

Порядок связности: orders (по стабильному хешу order_id) → order_items / order_payments /
order_reviews по order_id → customers по customer_id → products / sellers по позициям →
geolocation по zip-префиксам клиентов и продавцов (с потолком строк: префиксы грубые,
geolocation не сжимается пропорционально).

Выбор заказов — через стабильный хеш `md5(f"{seed}:{order_id}")`, а не позиционный `sample()`:
одно и то же множество order_id независимо от порядка строк и процесса.
"""

from __future__ import annotations

import hashlib

import pandas as pd

ORDERS = "orders"
ORDER_ITEMS = "order_items"
ORDER_PAYMENTS = "order_payments"
ORDER_REVIEWS = "order_reviews"
CUSTOMERS = "customers"
PRODUCTS = "products"
SELLERS = "sellers"
GEOLOCATION = "geolocation"

RAW_TABLES: tuple[str, ...] = (
    ORDERS,
    ORDER_ITEMS,
    ORDER_PAYMENTS,
    ORDER_REVIEWS,
    CUSTOMERS,
    SELLERS,
    PRODUCTS,
    GEOLOCATION,
)
# Имя файла Kaggle → имя raw-таблицы (= имя dbt source).
CSV_TO_TABLE: dict[str, str] = {f"olist_{t}_dataset.csv": t for t in RAW_TABLES}

_ORDER_CHILDREN = (ORDER_ITEMS, ORDER_PAYMENTS, ORDER_REVIEWS)
_HASH_SPACE = 2**32


def _stable_hash_mask(values: pd.Series, frac: float, seed: int) -> pd.Series:
    if frac >= 1.0:
        return pd.Series(True, index=values.index)
    if frac <= 0.0:
        return pd.Series(False, index=values.index)
    threshold = int(frac * _HASH_SPACE)

    def _h(v: str) -> int:
        return int(hashlib.md5(f"{seed}:{v}".encode()).hexdigest()[:8], 16)

    return values.astype(str).map(_h) < threshold


def sample_connected(
    tables: dict[str, pd.DataFrame],
    sample_frac: float,
    seed: int,
    geo_max_rows: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Вырезать ссылочно-целостный срез таблиц Olist. Входные frame'ы не мутируются."""
    if not (0.0 < sample_frac <= 1.0):
        raise ValueError(f"sample_frac must be in (0, 1], got {sample_frac!r}")
    if ORDERS not in tables:
        raise KeyError(f"tables must contain {ORDERS!r}")

    orders = tables[ORDERS]
    sel_orders = orders[_stable_hash_mask(orders["order_id"], sample_frac, seed)].copy()
    order_ids = set(sel_orders["order_id"])
    result: dict[str, pd.DataFrame] = {ORDERS: sel_orders}

    for name in _ORDER_CHILDREN:
        if name in tables:
            df = tables[name]
            result[name] = df[df["order_id"].isin(order_ids)].copy()

    sel_customers = None
    if CUSTOMERS in tables:
        customers = tables[CUSTOMERS]
        sel_customers = customers[
            customers["customer_id"].isin(set(sel_orders["customer_id"]))
        ].copy()
        result[CUSTOMERS] = sel_customers

    product_ids: set[str] = set()
    seller_ids: set[str] = set()
    if ORDER_ITEMS in result:
        product_ids = set(result[ORDER_ITEMS]["product_id"])
        seller_ids = set(result[ORDER_ITEMS]["seller_id"])
    if PRODUCTS in tables:
        products = tables[PRODUCTS]
        result[PRODUCTS] = products[products["product_id"].isin(product_ids)].copy()
    sel_sellers = None
    if SELLERS in tables:
        sellers = tables[SELLERS]
        sel_sellers = sellers[sellers["seller_id"].isin(seller_ids)].copy()
        result[SELLERS] = sel_sellers

    if GEOLOCATION in tables:
        zips: set[str] = set()
        if sel_customers is not None and "customer_zip_code_prefix" in sel_customers:
            zips |= set(sel_customers["customer_zip_code_prefix"].astype(str))
        if sel_sellers is not None and "seller_zip_code_prefix" in sel_sellers:
            zips |= set(sel_sellers["seller_zip_code_prefix"].astype(str))
        geo = tables[GEOLOCATION]
        sel_geo = geo[geo["geolocation_zip_code_prefix"].astype(str).isin(zips)]
        if geo_max_rows is not None and len(sel_geo) > geo_max_rows:
            # Точный потолок, стабильный к порядку файла: ранжируем строки по хешу содержимого
            # (префикс + координаты) и берём первые geo_max_rows.
            key = (
                sel_geo["geolocation_zip_code_prefix"].astype(str)
                + "|"
                + sel_geo["geolocation_lat"].astype(str)
                + "|"
                + sel_geo["geolocation_lng"].astype(str)
            )
            rank = key.map(lambda v: hashlib.md5(f"{seed}:{v}".encode()).hexdigest())
            sel_geo = sel_geo.assign(_rank=rank).sort_values("_rank", kind="mergesort")
            sel_geo = sel_geo.head(geo_max_rows).drop(columns="_rank")
        result[GEOLOCATION] = sel_geo.copy()

    for name, df in tables.items():
        if name not in result:
            result[name] = df.copy()
    return result


def frac_for_n_orders(n_orders_total: int, n_orders_wanted: int) -> float:
    """Доля заказов для sample_connected по желаемому числу (детерминизм — за хешем)."""
    if n_orders_wanted >= n_orders_total:
        return 1.0
    return n_orders_wanted / n_orders_total
