"""Измерить порог quality gate на фактическом snapshot (ADR-06): 5 сидов → ROC AUC holdout → min − 0.02.

uv run python scripts/measure_gate.py            # требует материализованной витрины (just feature-mart)
"""

from __future__ import annotations

import duckdb

from olist_ml.ml import features as F
from olist_ml.settings import settings

SEEDS = (1, 7, 42, 123, 2026)
MARGIN = 0.02


def main() -> None:
    con = duckdb.connect(str(settings.DUCKDB_PATH), read_only=True)
    cols = ", ".join([F.KEY, F.TARGET, *F.FEATURES])
    df = con.execute(
        f"select {cols} from marts.mart_order_features where {F.TARGET} is not null"
    ).df()
    con.close()
    aucs = []
    for seed in SEEDS:
        train, holdout = F.split_by_hash(df, settings.ML_TEST_FRAC, seed)
        pipe = F.build_pipeline(random_state=seed).fit(train[list(F.FEATURES)], train[F.TARGET])
        m = F.evaluate(
            pipe, holdout[list(F.FEATURES)], holdout[F.TARGET], settings.ML_SCORE_THRESHOLD
        )
        aucs.append(m["roc_auc"])
        print(
            f"seed={seed:5d}  roc_auc={m['roc_auc']:.4f}  pr_auc={m['pr_auc']:.4f}  n={m['n_rows']}"
        )
    print(f"rows={len(df)} positive_rate={df[F.TARGET].mean():.4f}")
    print(
        f"min={min(aucs):.4f} median={sorted(aucs)[len(aucs) // 2]:.4f} → ML_MIN_ROC_AUC={min(aucs) - MARGIN:.2f}"
    )


if __name__ == "__main__":
    main()
