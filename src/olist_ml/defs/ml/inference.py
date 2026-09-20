"""Контур B — минимальный batch inference (ADR-13): scoring_input → predictions.

- scoring_input: строки витрины с ещё неизвестным target (заказ не доставлен) — docs/contracts/features.md.
- predictions: один раз в начале run разрешает alias champion в версию, загружает полный Pipeline,
  считает score/predicted_class для batch_id; запись идемпотентна (delete where batch_id + insert);
  обучение не запускает; без champion — понятная ошибка. Прямой зависимости от ассетов обучения нет —
  общий контракт = схема признаков и preprocessing внутри Pipeline.
"""

import datetime as dt

import dagster as dg
import mlflow
import pandas as pd

from olist_ml.ml import features as F
from olist_ml.resources import DuckDBResource, MlflowResource
from olist_ml.settings import settings

MART = dg.AssetKey(["mart_order_features"])


class ScoreConfig(dg.Config):
    batch_id: str = settings.SCORING_BATCH_ID
    threshold: float = settings.ML_SCORE_THRESHOLD


@dg.asset(
    deps=[MART],
    group_name="inference",
    kinds={"duckdb"},
    owners=["team:data-engineering"],
    description="Строки витрины, у которых target ещё неизвестен (заказ не доставлен): вход для scoring.",
)
def scoring_input(duckdb: DuckDBResource, config: ScoreConfig) -> dg.Output[dict]:
    cols = ", ".join([F.KEY, *F.FEATURES])
    with duckdb.connect() as con:
        con.execute("create schema if not exists ml")
        con.execute("drop table if exists ml.scoring_input")
        con.execute(
            f"create table ml.scoring_input as "
            f"select '{config.batch_id}' as batch_id, {cols} "
            f"from marts.mart_order_features where {F.TARGET} is null order by {F.KEY}"
        )
        n = con.execute("select count(*) from ml.scoring_input").fetchone()[0]
    return dg.Output(
        {"batch_id": config.batch_id, "rows": n},
        metadata={"batch_id": config.batch_id, "rows": n, "table": "ml.scoring_input"},
    )


@dg.asset(
    group_name="inference",
    kinds={"sklearn", "mlflow", "duckdb"},
    owners=["team:data-engineering"],
    description=(
        "Скоринг опубликованной версией (models:/<name>@champion): score, predicted_class, "
        "model_version, batch_id, scored_at; повтор того же batch_id не даёт дублей."
    ),
)
def predictions(
    context: dg.AssetExecutionContext,
    scoring_input: dict,
    duckdb: DuckDBResource,
    mlflow_res: MlflowResource,
    config: ScoreConfig,
) -> dg.MaterializeResult:
    mlflow_res.setup()
    client = mlflow.MlflowClient()
    try:
        version = client.get_model_version_by_alias(mlflow_res.model_name, mlflow_res.model_alias)
    except mlflow.MlflowException as e:
        raise dg.Failure(
            description=(
                f"нет опубликованной модели: alias @{mlflow_res.model_alias} у "
                f"{mlflow_res.model_name!r} не установлен — выполните promote_job (just promote)"
            )
        ) from e
    model_version = int(version.version)
    pipe = mlflow.sklearn.load_model(f"models:/{mlflow_res.model_name}/{model_version}")
    context.log.info("champion → версия %s (run %s)", model_version, version.run_id)

    batch_id = scoring_input["batch_id"]
    with duckdb.connect() as con:
        df = con.execute(
            f"select {F.KEY}, {', '.join(F.FEATURES)} from ml.scoring_input "
            f"where batch_id = ? order by {F.KEY}",
            [batch_id],
        ).df()
        scores = pipe.predict_proba(df[list(F.FEATURES)])[:, 1] if len(df) else []
        out = pd.DataFrame(
            {
                F.KEY: df[F.KEY],
                "score": scores,
                "predicted_class": (pd.Series(scores) >= config.threshold).astype(int).values,
                "model_version": model_version,
                "batch_id": batch_id,
                "scored_at": dt.datetime.now(dt.UTC),
            }
        )
        con.execute(
            "create table if not exists ml.predictions ("
            "order_id varchar, score double, predicted_class integer, model_version integer, "
            "batch_id varchar, scored_at timestamp)"
        )
        con.execute("delete from ml.predictions where batch_id = ?", [batch_id])
        con.register("out_df", out)
        con.execute("insert into ml.predictions select * from out_df")
        total = con.execute("select count(*) from ml.predictions").fetchone()[0]
    return dg.MaterializeResult(
        metadata={
            "batch_id": batch_id,
            "model_version": model_version,
            "rows": len(out),
            "mean_score": round(float(out["score"].mean()), 4) if len(out) else 0.0,
            "predicted_positive_rate": round(float(out["predicted_class"].mean()), 4)
            if len(out)
            else 0.0,
            "rows_total_in_table": total,
            "registry_url": dg.MetadataValue.url(mlflow_res.model_url()),
            "note": "описательная статистика batch — не доказательство drift или качества",
        }
    )
