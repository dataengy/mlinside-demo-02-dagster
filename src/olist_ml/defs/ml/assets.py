"""Контур A — обучение (ADR-06, ADR-07, ADR-13): training_dataset → model → model_evaluation → model_registered.

- training_dataset: строки витрины с известным target, детерминированный сплит, snapshot train/holdout
  в parquet (evaluation читает snapshot, а не перечитывает витрину — §6.5 ревизии).
- model: один sklearn Pipeline (LogReg), MLflow run с параметрами, метрикой train и артефактом.
- model_evaluation: метрики на holdout; blocking gate — в checks.py.
- model_registered: версия в реестре БЕЗ алиаса; идемпотентно по dataset_fingerprint (promotion — M4).
"""

# Без `from __future__ import annotations`: Dagster резолвит типы dg.Config по реальным аннотациям.
import dagster as dg
import mlflow
import pandas as pd

from olist_ml.ml import features as F
from olist_ml.resources import DuckDBResource, MlflowResource, PathsResource
from olist_ml.settings import settings

MART = dg.AssetKey(["mart_order_features"])


class TrainConfig(dg.Config):
    test_frac: float = settings.ML_TEST_FRAC
    random_state: int = settings.RANDOM_STATE
    # Тег версии: baseline (demo-prepare, ADR-07a A) или candidate (кадр).
    role: str = "candidate"


@dg.asset(
    deps=[MART],
    group_name="ml",
    kinds={"pandas", "duckdb"},
    owners=["team:data-engineering"],
    description=(
        "Обучающая выборка: строки витрины с известным target, детерминированный сплит по md5(order_id); "
        "snapshot train/holdout сохраняется в parquet."
    ),
    # ADR-04a B: декларативный запуск только когда blocking-checks витрины прошли.
    automation_condition=dg.AutomationCondition.eager()
    & dg.AutomationCondition.all_deps_blocking_checks_passed(),
)
def training_dataset(
    duckdb: DuckDBResource, paths: PathsResource, config: TrainConfig
) -> dg.Output[dict]:
    cols = ", ".join([F.KEY, F.TARGET, *F.FEATURES, "order_purchase_timestamp"])
    with duckdb.connect(read_only=True) as con:
        df = con.execute(
            f"select {cols} from marts.mart_order_features where {F.TARGET} is not null order by {F.KEY}"
        ).df()
    params = {
        "test_frac": config.test_frac,
        "random_state": config.random_state,
        "features": F.FEATURES,
    }
    fp = F.dataset_fingerprint(df, params)
    train, holdout = F.split_by_hash(df, config.test_frac, config.random_state)
    out = paths.ml_dir()
    train_path, holdout_path = out / f"train_{fp}.parquet", out / f"holdout_{fp}.parquet"
    train.to_parquet(train_path, index=False)
    holdout.to_parquet(holdout_path, index=False)
    result = {
        "fingerprint": fp,
        "train_path": str(train_path),
        "holdout_path": str(holdout_path),
        "params": params,
    }
    return dg.Output(
        result,
        metadata={
            "dataset_fingerprint": fp,
            "rows_train": len(train),
            "rows_holdout": len(holdout),
            "positive_rate": round(float(df[F.TARGET].mean()), 4),
            "period": f"{df['order_purchase_timestamp'].min()} … {df['order_purchase_timestamp'].max()}",
            "features": dg.MetadataValue.json(list(F.FEATURES)),
        },
    )


@dg.asset(
    group_name="ml",
    kinds={"sklearn", "mlflow"},
    owners=["team:data-engineering"],
    description="Кандидат: sklearn Pipeline (preprocessing + LogisticRegression), fit только на train; MLflow run.",
)
def model(
    training_dataset: dict, mlflow_res: MlflowResource, config: TrainConfig
) -> dg.Output[dict]:
    exp_id = mlflow_res.setup()
    train = pd.read_parquet(training_dataset["train_path"])
    x, y = train[list(F.FEATURES)], train[F.TARGET]
    pipe = F.build_pipeline(random_state=config.random_state)
    params = {
        "model": "logreg",
        "random_state": config.random_state,
        "test_frac": config.test_frac,
        "n_features": len(F.FEATURES),
    }
    with mlflow.start_run(
        tags={"dataset_fingerprint": training_dataset["fingerprint"], "role": config.role}
    ) as run:
        pipe.fit(x, y)
        train_metrics = F.evaluate(pipe, x, y, settings.ML_SCORE_THRESHOLD)
        mlflow.log_params(params)
        mlflow.log_metric("roc_auc_train", train_metrics["roc_auc"])
        info = mlflow.sklearn.log_model(
            pipe,
            name="model",
            serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE,
            input_example=x.head(5),
        )
    result = {
        "run_id": run.info.run_id,
        "model_uri": info.model_uri,
        "exp_id": exp_id,
        "fingerprint": training_dataset["fingerprint"],
    }
    return dg.Output(
        result,
        metadata={
            "mlflow_url": dg.MetadataValue.url(mlflow_res.run_url(exp_id, run.info.run_id)),
            "run_id": run.info.run_id,
            "roc_auc_train": round(train_metrics["roc_auc"], 4),
            **params,
        },
    )


@dg.asset(
    group_name="ml",
    kinds={"sklearn"},
    owners=["team:data-engineering"],
    description=(
        "Оценка на holdout-снапшоте того же fingerprint (витрина не перечитывается); "
        "quality_gate — blocking check на этом ассете."
    ),
)
def model_evaluation(
    model: dict, training_dataset: dict, mlflow_res: MlflowResource
) -> dg.Output[dict]:
    mlflow_res.setup()
    holdout = pd.read_parquet(training_dataset["holdout_path"])
    pipe = mlflow.sklearn.load_model(model["model_uri"])
    metrics = F.evaluate(
        pipe, holdout[list(F.FEATURES)], holdout[F.TARGET], settings.ML_SCORE_THRESHOLD
    )
    with mlflow.start_run(run_id=model["run_id"]):
        mlflow.log_metrics({f"{k}_holdout": v for k, v in metrics.items() if k != "n_rows"})
    result = {**metrics, "run_id": model["run_id"], "fingerprint": model["fingerprint"]}
    rounded = {k: round(v, 4) for k, v in metrics.items() if isinstance(v, float)}
    return dg.Output(
        result,
        metadata={
            **rounded,
            "rows_holdout": metrics["n_rows"],
            "mlflow_url": dg.MetadataValue.url(
                mlflow_res.run_url(model["exp_id"], model["run_id"])
            ),
        },
    )


@dg.asset(
    group_name="ml",
    kinds={"mlflow"},
    owners=["team:data-engineering"],
    description=(
        "Версия в реестре MLflow БЕЗ алиаса (promotion — отдельное действие); "
        "идемпотентно: тот же dataset_fingerprint → версия не создаётся."
    ),
)
def model_registered(
    model: dict, model_evaluation: dict, mlflow_res: MlflowResource
) -> dg.MaterializeResult:
    mlflow_res.setup()
    client = mlflow.MlflowClient()
    name, fp = mlflow_res.model_name, model["fingerprint"]
    existing = next(
        (
            v
            for v in client.search_model_versions(f"name='{name}'")
            if client.get_run(v.run_id).data.tags.get("dataset_fingerprint") == fp
        ),
        None,
    )
    if existing is None:
        version = mlflow.register_model(model["model_uri"], name, tags={"dataset_fingerprint": fp})
    else:
        version = existing
    return dg.MaterializeResult(
        metadata={
            "model_name": name,
            "model_version": int(version.version),
            "reused_existing": existing is not None,
            "roc_auc_holdout": round(model_evaluation["roc_auc"], 4),
            "registry_url": dg.MetadataValue.url(mlflow_res.model_url()),
            "note": "алиас champion ставит promote_job (M4); 'последняя версия' ≠ production",
        }
    )
