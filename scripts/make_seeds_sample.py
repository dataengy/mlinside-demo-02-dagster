"""Shipped snapshot raw-таблиц Olist → dbt/seeds/raw/<table>.csv (и фикстуры тестов) — ADR-03.

Вход — полные CSV Kaggle (`olist_*_dataset.csv`); по умолчанию — из seeds/ канонического
проекта или из --source-dir. Выход детерминирован: тот же RANDOM_STATE → те же строки.

    uv run python scripts/make_seeds_sample.py            # snapshot + фикстуры из settings
    uv run python scripts/make_seeds_sample.py --source-dir data/full --n-orders 2000
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from olist_ml.sampling import CSV_TO_TABLE, GEOLOCATION, ORDERS, frac_for_n_orders, sample_connected
from olist_ml.settings import PROJECT_ROOT, settings

DEFAULT_SOURCE = PROJECT_ROOT.parent / "mlinside-hw-olist" / "dbt" / "seeds"
GEO_MAX_ROWS_PER_ORDER = 4  # потолок geolocation: ~4 строки на заказ (в полном датасете ~10)


def read_full(source_dir: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    for csv_name, table in CSV_TO_TABLE.items():
        path = source_dir / csv_name
        if not path.is_file():
            raise FileNotFoundError(f"{path} — нужен полный датасет Kaggle (9 CSV)")
        tables[table] = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    return tables


def write_sample(
    tables: dict[str, pd.DataFrame], n_orders: int, seed: int, out_dir: Path
) -> dict[str, int]:
    frac = frac_for_n_orders(len(tables[ORDERS]), n_orders)
    sampled = sample_connected(tables, frac, seed, geo_max_rows=n_orders * GEO_MAX_ROWS_PER_ORDER)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for table, df in sampled.items():
        df = df.sort_values(
            list(df.columns), kind="mergesort"
        )  # стабильный порядок строк → стабильный diff
        df.to_csv(out_dir / f"{table}.csv", index=False)
        counts[table] = len(df)
    return counts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    p.add_argument("--n-orders", type=int, default=settings.SNAPSHOT_N_ORDERS)
    p.add_argument("--fixtures-n-orders", type=int, default=settings.FIXTURES_N_ORDERS)
    p.add_argument("--seed", type=int, default=settings.RANDOM_STATE)
    p.add_argument("--out", type=Path, default=settings.DBT_PROJECT_DIR / "seeds" / "raw")
    p.add_argument("--fixtures-out", type=Path, default=PROJECT_ROOT / "tests" / "fixtures" / "raw")
    args = p.parse_args()

    tables = read_full(args.source_dir)
    for label, n, out in (
        ("snapshot", args.n_orders, args.out),
        ("fixtures", args.fixtures_n_orders, args.fixtures_out),
    ):
        counts = write_sample(tables, n, args.seed, out)
        print(f"{label} → {out}: " + ", ".join(f"{t}={c}" for t, c in counts.items()))
        assert counts[GEOLOCATION] <= n * GEO_MAX_ROWS_PER_ORDER


if __name__ == "__main__":
    main()
