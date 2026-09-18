#!/usr/bin/env python3
"""Прототип ML-задачи демо 3: «заказ будет доставлен с опозданием».

Проверяет на реальных данных то, что потом станет ассетами Dagster
(training_dataset -> model -> model_evaluation): сколько длится обучение,
какие метрики достижимы, где ставить quality gate и сколько стоит PNG
для MetadataValue.png.

Запуск:
  uv run --python 3.12 --with duckdb --with pandas --with scikit-learn \
      --with pyarrow --with matplotlib python prototype_train.py \
      --db /path/olist.duckdb --sample-frac 0.2 --seed 42
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "mplconfig"))

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent))
from features_sql import build_sql  # noqa: E402

TARGET = "is_late_delivery"
KEY = "order_id"

NUM_FEATURES = [
    "items_cnt",
    "distinct_products_cnt",
    "distinct_sellers_cnt",
    "items_value",
    "freight_value",
    "order_value",
    "freight_share",
    "is_large_order",
    "max_installments",
    "customer_seller_distance_km",
    "order_hour",
    "order_dow",
    "purchase_month",
    "estimated_delivery_span_days",
    "total_weight_g",
    "total_volume_cm3",
    "max_item_weight_g",
]
CAT_FEATURES = [
    "main_payment_type",
    "customer_state",
    "customer_region",
    "main_product_category",
]
FEATURES = NUM_FEATURES + CAT_FEATURES


# --------------------------------------------------------------------------
# Детерминированное сэмплирование и сплит: bucket = md5(order_id|salt) % 10000.
# Именно md5, а не встроенный hash(): PYTHONHASHSEED рандомизируется на каждый
# процесс, и встроенный hash дал бы разный holdout при каждом запуске Dagster.
# --------------------------------------------------------------------------
def hash_bucket(values: pd.Series, salt: str, mod: int = 10_000) -> np.ndarray:
    return np.fromiter(
        (
            int(hashlib.md5(f"{v}|{salt}".encode()).hexdigest()[:8], 16) % mod
            for v in values
        ),
        dtype=np.int64,
        count=len(values),
    )


def load_features(db_path: str, sch_stg: str, sch_int: str) -> pd.DataFrame:
    try:
        con = duckdb.connect(db_path, read_only=True)
    except duckdb.IOException:
        tmp = Path(tempfile.gettempdir()) / "olist_copy.duckdb"
        shutil.copy2(db_path, tmp)
        con = duckdb.connect(str(tmp), read_only=True)
    try:
        return con.execute(build_sql(sch_stg=sch_stg, sch_int=sch_int)).fetchdf()
    finally:
        con.close()


def build_logreg() -> Pipeline:
    return Pipeline(
        [
            (
                "prep",
                ColumnTransformer(
                    [
                        (
                            "num",
                            Pipeline(
                                [
                                    ("imp", SimpleImputer(strategy="median")),
                                    ("sc", StandardScaler()),
                                ]
                            ),
                            NUM_FEATURES,
                        ),
                        (
                            "cat",
                            OneHotEncoder(handle_unknown="ignore", min_frequency=20),
                            CAT_FEATURES,
                        ),
                    ]
                ),
            ),
            ("clf", LogisticRegression(max_iter=1000)),
        ]
    )


def build_hgb(seed: int) -> Pipeline:
    cat_idx = [NUM_FEATURES.__len__() + i for i in range(len(CAT_FEATURES))]
    return Pipeline(
        [
            (
                "prep",
                ColumnTransformer(
                    [
                        ("num", "passthrough", NUM_FEATURES),
                        (
                            "cat",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value", unknown_value=-1
                            ),
                            CAT_FEATURES,
                        ),
                    ]
                ),
            ),
            (
                "clf",
                # Дефолты (max_iter=200, max_leaf_nodes=31) на этом датасете дают
                # train ROC AUC = 1.0 при holdout 0.70 — чистое переобучение и
                # лишние секунды. Ограничение листьев и min_samples_leaf убирают
                # переобучение И ускоряют обучение примерно вчетверо.
                HistGradientBoostingClassifier(
                    max_iter=100,
                    learning_rate=0.1,
                    max_leaf_nodes=15,
                    min_samples_leaf=50,
                    l2_regularization=1.0,
                    early_stopping=False,
                    categorical_features=cat_idx,
                    random_state=seed,
                ),
            ),
        ]
    )


def evaluate(model, X, y) -> dict:
    proba = model.predict_proba(X)[:, 1]
    pred = (proba >= 0.5).astype(int)
    return {
        "roc_auc": float(roc_auc_score(y, proba)),
        "pr_auc": float(average_precision_score(y, proba)),
        "accuracy": float(accuracy_score(y, pred)),
        "positive_rate_true": float(y.mean()),
        "positive_rate_pred": float(pred.mean()),
        "_proba": proba,
    }


def logreg_top(model, k=10) -> list[tuple[str, float]]:
    names = model.named_steps["prep"].get_feature_names_out()
    coefs = model.named_steps["clf"].coef_[0]
    order = np.argsort(np.abs(coefs))[::-1][:k]
    return [(str(names[i]).split("__", 1)[-1], float(coefs[i])) for i in order]


def perm_top(model, X, y, seed, k=10, n_repeats=3, max_rows=20_000):
    if len(X) > max_rows:
        Xs, ys = X.iloc[:max_rows], y.iloc[:max_rows]
    else:
        Xs, ys = X, y
    r = permutation_importance(
        model, Xs, ys, scoring="roc_auc", n_repeats=n_repeats, random_state=seed, n_jobs=1
    )
    order = np.argsort(r.importances_mean)[::-1][:k]
    return [(FEATURES[i], float(r.importances_mean[i])) for i in order]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="/Users/user/gi/@dataengy/mlinside-hw-olist/dbt/olist.duckdb")
    p.add_argument("--sample-frac", type=float, default=0.2)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--test-frac", type=float, default=0.2)
    p.add_argument("--schema-staging", default="main_staging")
    p.add_argument("--schema-intermediate", default="main_intermediate")
    p.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "artifacts"))
    p.add_argument("--no-plots", action="store_true")
    p.add_argument("--no-perm", action="store_true")
    a = p.parse_args()

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    tag = f"frac{a.sample_frac}_seed{a.seed}"

    t = time.perf_counter()
    df = load_features(a.db, a.schema_staging, a.schema_intermediate)
    sql_sec = time.perf_counter() - t
    rows_total = len(df)

    # Сэмплирование и сплит — по разным солям, иначе holdout окажется
    # вложенным в сэмпл неслучайным образом.
    if a.sample_frac < 1.0:
        keep = hash_bucket(df[KEY], f"sample|{a.seed}") < int(a.sample_frac * 10_000)
        df = df.loc[keep]
    df = df.sort_values(KEY, kind="mergesort").reset_index(drop=True)

    is_test = hash_bucket(df[KEY], f"split|{a.seed}") < int(a.test_frac * 10_000)
    train, test = df.loc[~is_test], df.loc[is_test]

    Xtr, ytr = train[FEATURES], train[TARGET]
    Xte, yte = test[FEATURES], test[TARGET]

    fingerprint = hashlib.md5(
        (
            ",".join(train[KEY].tolist()) + "|" + ",".join(test[KEY].tolist())
        ).encode()
    ).hexdigest()

    res = {
        "sample_frac": a.sample_frac,
        "seed": a.seed,
        "rows_total": rows_total,
        "rows_sampled": len(df),
        "rows_train": len(train),
        "rows_test": len(test),
        "sql_sec": round(sql_sec, 3),
        "dataset_fingerprint": fingerprint,
        "positive_rate_all": float(df[TARGET].mean()),
        "models": {},
    }

    rocs = {}
    for name, model in (("logreg", build_logreg()), ("hgb", build_hgb(a.seed))):
        t = time.perf_counter()
        model.fit(Xtr, ytr)
        fit_sec = time.perf_counter() - t
        t = time.perf_counter()
        m = evaluate(model, Xte, yte)
        pred_sec = time.perf_counter() - t
        rocs[name] = m.pop("_proba")
        m["fit_sec"] = round(fit_sec, 3)
        m["predict_sec"] = round(pred_sec, 3)
        m["train_roc_auc"] = float(roc_auc_score(ytr, model.predict_proba(Xtr)[:, 1]))
        if name == "logreg":
            m["top10_coef"] = logreg_top(model)
        if not a.no_perm:
            t = time.perf_counter()
            m["top10_permutation"] = perm_top(model, Xte, yte, a.seed)
            m["perm_sec"] = round(time.perf_counter() - t, 3)
        res["models"][name] = m

    if not a.no_plots:
        t = time.perf_counter()
        fig, ax = plt.subplots(figsize=(5.5, 4.5), dpi=110)
        for name, proba in rocs.items():
            fpr, tpr, _ = roc_curve(yte, proba)
            ax.plot(fpr, tpr, label=f"{name} (AUC={roc_auc_score(yte, proba):.3f})")
        ax.plot([0, 1], [0, 1], "k--", lw=0.8)
        ax.set_xlabel("FPR"); ax.set_ylabel("TPR")
        ax.set_title(f"ROC — late delivery, sample_frac={a.sample_frac}")
        ax.legend(loc="lower right"); fig.tight_layout()
        roc_png = out / f"roc_{tag}.png"
        fig.savefig(roc_png); plt.close(fig)

        imp = res["models"]["hgb"].get("top10_permutation") or res["models"]["logreg"]["top10_coef"]
        fig, ax = plt.subplots(figsize=(6.5, 4.5), dpi=110)
        names = [n for n, _ in imp][::-1]
        vals = [v for _, v in imp][::-1]
        ax.barh(names, vals, color="#4c78a8")
        ax.set_title("Top-10 features: permutation importance\n(падение ROC AUC)", fontsize=10)
        fig.tight_layout()
        imp_png = out / f"importance_{tag}.png"
        fig.savefig(imp_png); plt.close(fig)
        res["png_sec"] = round(time.perf_counter() - t, 3)
        res["png"] = [str(roc_png), str(imp_png)]
        res["png_bytes"] = [roc_png.stat().st_size, imp_png.stat().st_size]

    (out / f"metrics_{tag}.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(json.dumps(res, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
