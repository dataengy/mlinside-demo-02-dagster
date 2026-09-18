"""Смоук-прогон kaggle_olist_source -> DuckDB.

Вспомогательный скрипт лейна (не часть компонента/декоратора), нужен только
чтобы замерить row counts/время п.3 задачи и проверить идемпотентность
write_disposition="replace" повторным запуском. Пишет DuckDB-файл в $TMPDIR
(не в LANE — LANE только для исходников черновика и отчёта).

Запуск:
    export SMOKE_DUCKDB_PATH=$TMPDIR/olist_ingest_smoke/olist.duckdb
    PYTHONPATH=<LANE> uv run --no-project --python 3.12 \\
      --with "dlt[duckdb]==1.30.0" --with "duckdb==1.5.5" --with "kagglehub==1.0.2" \\
      --with pandas --with pyarrow \\
      python _smoke_run_pipeline.py
"""

from __future__ import annotations

import os
import time

import duckdb

from kaggle_olist_source import CSV_BY_RESOURCE, kaggle_olist_source, pipeline

DUCKDB_PATH = os.environ["SMOKE_DUCKDB_PATH"]
DATASET_NAME = "olist_raw"
SAMPLE_FRAC = float(os.environ.get("SMOKE_SAMPLE_FRAC", "0.02"))
SEED = int(os.environ.get("SMOKE_SEED", "42"))


def _row_counts() -> dict[str, int]:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        tables = con.execute(
            "select table_name from information_schema.tables where table_schema = ? order by table_name",
            [DATASET_NAME],
        ).fetchall()
        counts: dict[str, int] = {}
        for (t,) in tables:
            counts[t] = con.execute(f'select count(*) from "{DATASET_NAME}"."{t}"').fetchone()[0]
        return counts
    finally:
        con.close()


def main() -> None:
    print(f"DUCKDB_PATH={DUCKDB_PATH}")
    print(f"SAMPLE_FRAC={SAMPLE_FRAC} SEED={SEED}")

    pl = pipeline(DUCKDB_PATH, dataset_name=DATASET_NAME)

    t0 = time.time()
    info1 = pl.run(kaggle_olist_source(sample_frac=SAMPLE_FRAC, seed=SEED))
    t1 = time.time()
    print(f"RUN1_SECONDS={t1 - t0:.3f}")
    print(f"RUN1_LOAD_INFO={info1}")

    counts1 = _row_counts()
    for name in CSV_BY_RESOURCE:
        print(f"RUN1_COUNT {name} {counts1.get(name, 'MISSING')}")
    print(f"RUN1_TABLES_PRESENT={sorted(counts1) == sorted(CSV_BY_RESOURCE)}")

    t2 = time.time()
    info2 = pl.run(kaggle_olist_source(sample_frac=SAMPLE_FRAC, seed=SEED))
    t3 = time.time()
    print(f"RUN2_SECONDS={t3 - t2:.3f}")
    print(f"RUN2_LOAD_INFO={info2}")

    counts2 = _row_counts()
    all_match = True
    for name in CSV_BY_RESOURCE:
        c1 = counts1.get(name)
        c2 = counts2.get(name)
        match = c1 == c2
        all_match = all_match and match
        print(f"RUN2_COUNT {name} {c2} MATCH={match}")
    print(f"IDEMPOTENT_REPLACE={all_match}")


if __name__ == "__main__":
    main()
