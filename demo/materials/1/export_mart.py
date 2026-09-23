"""Демо 1, подготовка: выгрузить витрину marts.mart_order_features из DuckDB в parquet.

Запуск из корня репозитория (окружение direnv активно), после `just demo-prepare && just feature-mart`:
    python demo/materials/1/export_mart.py demo/data/mart_order_features.parquet
"""

import sys
from pathlib import Path

import duckdb

db = Path("data/olist.duckdb")
out = Path(sys.argv[1] if len(sys.argv) > 1 else "demo/data/mart_order_features.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
with duckdb.connect(str(db), read_only=True) as con:
    con.execute(
        f"copy (select * from marts.mart_order_features order by order_id) to '{out}' (format parquet)"
    )
    n = con.execute(f"select count(*) from read_parquet('{out}')").fetchone()[0]
print(f"{out}: {n} rows")
