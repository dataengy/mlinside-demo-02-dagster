"""Детерминированное связное сэмплирование таблиц Olist.

Черновик для dlt-источника ``kaggle_olist`` (демо 1 v2, см. PROMPT.md — раздел
«Демо 1 — инициализация проекта и ingest», п. «Сэмплирование»). Логика — прямой
перенос ``scripts/make_sample.py`` (mlinside-hw-olist), но обобщённый на
произвольный набор уже загруженных в память DataFrame'ов (там, где make_sample.py
читает CSV с диска, здесь на вход подаётся dict {имя_таблицы: DataFrame} —
это нужно, чтобы вызвать сэмплирование ОДИН раз внутри @dlt.source(), до yield
любого @dlt.resource, и замкнуть результат в каждый ресурс).

Порядок связности (как в make_sample.py):

    orders (sample_frac от всех заказов, детерминированно по seed)
      -> order_items / order_payments / order_reviews   по order_id
      -> customers                                       по customer_id
      -> products / sellers                              по product_id / seller_id
      -> geolocation                                      по zip-префиксам отобранных
                                                            customers и sellers
      -> product_category_name_translation                словарь, не режется (мелкий:
                                                            ~70 строк), проходит целиком

Выбор заказов — через СТАБИЛЬНЫЙ ХЕШ ``order_id``, а не позиционный
``DataFrame.sample(random_state=...)``. Разница важна: хеш даёт одно и то же
множество ``order_id`` независимо от порядка строк и от того, сколько раз и в
каком процессе вызвана функция — то есть детерминизм не только «тот же seed
→ тот же результат при одинаковом входе», но и устойчивость к перестановке
строк входного DataFrame. Для dlt-источника, где каждый ресурс мог бы в теории
получить свою копию таблицы заказов (например, после повторного чтения CSV),
это более надёжная гарантия связности, чем ``random_state`` от позиции.
"""

from __future__ import annotations

import hashlib

import pandas as pd

ORDERS_TABLE = "orders"
ORDER_ITEMS_TABLE = "order_items"
ORDER_PAYMENTS_TABLE = "order_payments"
ORDER_REVIEWS_TABLE = "order_reviews"
CUSTOMERS_TABLE = "customers"
PRODUCTS_TABLE = "products"
SELLERS_TABLE = "sellers"
GEOLOCATION_TABLE = "geolocation"
CATEGORY_TRANSLATION_TABLE = "product_category_name_translation"

# Таблицы, которые фильтруются напрямую по order_id выбранных заказов.
_ORDER_CHILD_TABLES = (ORDER_ITEMS_TABLE, ORDER_PAYMENTS_TABLE, ORDER_REVIEWS_TABLE)

_HASH_SPACE = 2**32  # первые 8 hex-символов md5 -> 32-битное целое


def _stable_hash_frac(values: pd.Series, frac: float, seed: int) -> pd.Series:
    """Детерминированная булева маска: ~frac от `values` через стабильный хеш.

    md5(f"{seed}:{value}") -> первые 32 бита -> сравнение с порогом frac*2**32.
    Не заглядывает в порядок/длину values, поэтому один и тот же order_id
    получает один и тот же вердикт (включён/выключен) независимо от контекста
    вызова — это и есть требуемая детерминированность связного сэмпла.
    """
    if frac >= 1.0:
        return pd.Series(True, index=values.index)
    if frac <= 0.0:
        return pd.Series(False, index=values.index)
    threshold = int(frac * _HASH_SPACE)

    def _h(v: str) -> int:
        digest = hashlib.md5(f"{seed}:{v}".encode("utf-8")).hexdigest()
        return int(digest[:8], 16)

    return values.astype(str).map(_h) < threshold


def sample_connected(
    tables: dict[str, pd.DataFrame],
    sample_frac: float,
    seed: int,
) -> dict[str, pd.DataFrame]:
    """Вырезать ссылочно-целостный срез таблиц Olist.

    Параметры
    ---------
    tables:
        {имя_таблицы: DataFrame}. Обязателен ``"orders"``; остальные ключи из
        {order_items, order_payments, order_reviews, customers, products,
        sellers, geolocation, product_category_name_translation} — опциональны,
        отсутствующие в `tables` просто отсутствуют в результате. Таблицы, не
        входящие в этот список (неизвестные ключи), проходят как есть (copy),
        без фильтрации — на случай будущих ресурсов.
    sample_frac:
        Доля заказов, (0, 1]. 1.0 -> вернуть все таблицы без изменений
        (копиями), это фиксирует поведение "полный объём — только по явному
        конфигу" из PROMPT.md.
    seed:
        Соль стабильного хеша; тот же seed на тех же входных данных даёт то
        же множество отобранных order_id.

    Возвращает
    ----------
    Новый dict {имя_таблицы: DataFrame} (копии, входные frame'ы не мутируются).
    """
    if not (0.0 < sample_frac <= 1.0):
        raise ValueError(f"sample_frac must be in (0, 1], got {sample_frac!r}")
    if ORDERS_TABLE not in tables:
        raise KeyError(f"tables must contain {ORDERS_TABLE!r}")

    orders = tables[ORDERS_TABLE]
    mask = _stable_hash_frac(orders["order_id"], sample_frac, seed)
    sel_orders = orders[mask].copy()
    order_ids = set(sel_orders["order_id"])

    result: dict[str, pd.DataFrame] = {ORDERS_TABLE: sel_orders}

    for name in _ORDER_CHILD_TABLES:
        if name in tables:
            df = tables[name]
            result[name] = df[df["order_id"].isin(order_ids)].copy()

    sel_customers = None
    if CUSTOMERS_TABLE in tables:
        customer_ids = set(sel_orders["customer_id"])
        customers = tables[CUSTOMERS_TABLE]
        sel_customers = customers[customers["customer_id"].isin(customer_ids)].copy()
        result[CUSTOMERS_TABLE] = sel_customers

    product_ids: set[str] = set()
    seller_ids: set[str] = set()
    if ORDER_ITEMS_TABLE in result:
        product_ids = set(result[ORDER_ITEMS_TABLE]["product_id"])
        seller_ids = set(result[ORDER_ITEMS_TABLE]["seller_id"])

    sel_sellers = None
    if PRODUCTS_TABLE in tables:
        products = tables[PRODUCTS_TABLE]
        result[PRODUCTS_TABLE] = products[products["product_id"].isin(product_ids)].copy()

    if SELLERS_TABLE in tables:
        sellers = tables[SELLERS_TABLE]
        sel_sellers = sellers[sellers["seller_id"].isin(seller_ids)].copy()
        result[SELLERS_TABLE] = sel_sellers

    if GEOLOCATION_TABLE in tables:
        zips: set[str] = set()
        if sel_customers is not None and "customer_zip_code_prefix" in sel_customers.columns:
            zips |= set(sel_customers["customer_zip_code_prefix"])
        if sel_sellers is not None and "seller_zip_code_prefix" in sel_sellers.columns:
            zips |= set(sel_sellers["seller_zip_code_prefix"])
        geo = tables[GEOLOCATION_TABLE]
        result[GEOLOCATION_TABLE] = geo[geo["geolocation_zip_code_prefix"].isin(zips)].copy()

    if CATEGORY_TRANSLATION_TABLE in tables:
        # Словарь категорий ~70 строк — фильтровать не даёт ничего, кроме риска
        # потерять перевод категории; проходит целиком, как в make_sample.py
        # (там этот файл вообще не участвует в сэмплировании).
        result[CATEGORY_TRANSLATION_TABLE] = tables[CATEGORY_TRANSLATION_TABLE].copy()

    # Не входит в список выше, но присутствует на входе — пропускаем как есть
    # (future-proofing на случай нового ресурса).
    for name, df in tables.items():
        if name not in result:
            result[name] = df.copy()

    return result
