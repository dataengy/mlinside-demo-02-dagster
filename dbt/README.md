# dbt/

dbt-проект: raw → staging → intermediate → marts, читается DuckDB (`data/olist.duckdb`).

## Статус: синхронизация с каноном отключена

Раньше `dbt/` был копией канонического проекта `mlinside-hw-olist/dbt` (ADR-04, разрешённые
отличия из `scripts/dbt_overlay/`), обновлялся через `just dbt-sync`. Сейчас `scripts/sync_dbt_from_canonical.sh`
завершается с ошибкой — синхронизация приостановлена, пока `dbt/` упрощается до одной витрины
`mart_order_features` и её upstream-моделей. Пока это так, модели здесь можно править напрямую.

## Слои

| Слой | Файлы |
|---|---|
| `models/sources/sources.yml` | описание raw-источников (снапшот из `dbt/seeds/raw`) |
| `models/staging/stg_*.sql` | по одной модели на raw-таблицу: customers, geolocation, order_items, order_payments, order_reviews, orders, products, sellers |
| `models/intermediate/int_*.sql` | `int_orders_enriched`, `int_order_items_enriched`, `int_order_distance` |
| `models/marts/*.sql` | целевая витрина `mart_order_features` (используется ML-ассетами) + прочие: `mart_customer_value_profile`, `mart_daily_state_metrics`, `mart_delivery_quality_by_category`, `mart_delivery_quality_by_state`, `mart_hourly_order_pattern`, `mart_seller_analytics` — кандидаты на удаление при упрощении, не источник для ML |
| `models/py/my_python_model.py` | Python-модель dbt (пример) |
| `seeds/raw/*.csv` | snapshot из `scripts/make_seeds_sample.py` (ADR-03) — не редактировать руками |
| `seeds/product_category_name_translation.csv`, `seeds/seed_state_region.csv` | справочники |

## Не коммитится

`target/`, `dbt_packages/`, `logs/`, `.env`, `.user.yml` (`dbt/.gitignore`).

## Команды

`just dbt-deps` / `just dbt-parse` (манифест без обращения к БД/сети) / `just dbt-build` /
`just feature-mart`. После правки SQL: `just dbt-parse` + Reload definitions в UI (ADR-04b).
