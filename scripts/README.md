# scripts/

Вспомогательные скрипты, вызываются через `just`, а не напрямую, где для них есть рецепт.

| Скрипт | Зачем | Команда | ADR |
|---|---|---|---|
| `sync_dbt_from_canonical.sh` | ⚠ **временно отключён** (завершается с ошибкой) — копировал `dbt/` из канонического проекта `mlinside-hw-olist/dbt` + наложение `dbt_overlay/`. Отключён, пока `dbt/` упрощается до одной витрины `mart_order_features` и её upstream: полный rsync стёр бы урезанное дерево | `just dbt-sync` | ADR-04 |
| `make_seeds_sample.py` | Детерминированный snapshot raw-таблиц Olist → `dbt/seeds/raw/*.csv` и фикстуры тестов | `just seeds-sample` | ADR-03 |
| `measure_gate.py` | Измерить порог quality gate (ROC AUC) на фактическом snapshot: 5 сидов → min − 0.02 | `just measure-gate` | ADR-06 |
| `demo_patch.py` | Заготовленные патчи для демо-сценариев: `break` (дубли order_id), `sql-change` (безобидная правка SQL), `break-warn`, `fix` | `just demo-break` / `demo-sql-change` / `demo-break-warn` / `demo-fix` | сценарии DEMO S6/S16/S20 |

## `dbt_overlay/`

Разрешённые отличия `dbt/` от канонического проекта: `models/marts/mart_order_features.*`,
`models/sources/sources.yml`, `macros/generate_schema_name.sql`, `macros/cross_db/date_parts.sql`.
Раньше накладывались поверх канона при `just dbt-sync`. Пока sync отключён (см. выше), этот каталог
не применяется — состояние `dbt/` актуальнее overlay. Не удалять: источник правды для разрешённых
отличий, если синхронизацию с каноном вернут.
