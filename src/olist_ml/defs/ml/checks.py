"""Quality gate — единственный blocking check ML (ADR-06): ROC AUC на holdout ≥ порога.

Порог по умолчанию — settings.ML_MIN_ROC_AUC (измерен на shipped snapshot); для демо «gate падает»
порог поднимается через run config (`quality_gate: {config: {min_roc_auc: 0.99}}`), а не «модель похуже».
"""

import dagster as dg

from olist_ml.defs.ml.assets import model_evaluation
from olist_ml.ml import features as F
from olist_ml.settings import settings


class GateConfig(dg.Config):
    min_roc_auc: float = settings.ML_MIN_ROC_AUC


@dg.asset_check(
    asset=model_evaluation,
    blocking=True,
    description="Quality gate: ROC AUC на holdout не ниже порога; провал останавливает model_registered.",
)
def quality_gate(model_evaluation: dict, config: GateConfig) -> dg.AssetCheckResult:
    value = model_evaluation["roc_auc"]
    return dg.AssetCheckResult(
        passed=F.gate(value, config.min_roc_auc),
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"roc_auc_holdout": round(value, 4), "threshold": config.min_roc_auc},
    )
