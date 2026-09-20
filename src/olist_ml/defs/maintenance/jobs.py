"""Очистка данных — три уровня (ADR-10): raw | derived | all. Единственное законное место @op/@job
(вместе с promotion): джобы ничего не материализуют. Каждая идемпотентна — на пустом состоянии успех.

clean_raw_job      схема raw (пересеивается demo_prepare)
clean_derived_job  схемы staging/intermediate/marts/seeds/test_failures/ml, data/ml/*.parquet,
                   MLflow: эксперимент + registered model (+ артефакты), dbt/target
clean_all_job      raw + derived
"""

import shutil
from pathlib import Path

import dagster as dg
import mlflow

from olist_ml.resources import DuckDBResource, MlflowResource, PathsResource
from olist_ml.settings import settings

DERIVED_SCHEMAS = ("staging", "intermediate", "marts", "seeds", "test_failures", "ml")


def _drop_schemas(
    duckdb: DuckDBResource, log: dg.DagsterLogManager, schemas: tuple[str, ...]
) -> list[str]:
    if not Path(duckdb.path).exists():
        log.info("DuckDB %s ещё нет — нечего чистить", duckdb.path)
        return []
    dropped = []
    with duckdb.connect() as con:
        existing = {
            r[0]
            for r in con.execute("select schema_name from information_schema.schemata").fetchall()
        }
        for s in schemas:
            if s in existing:
                con.execute(f"drop schema {s} cascade")
                dropped.append(s)
    log.info("дропнуты схемы: %s", dropped or "—")
    return dropped


@dg.op(description="Дропнуть схему raw в DuckDB (snapshot пересеивается demo_prepare_job).")
def clean_raw(context: dg.OpExecutionContext, duckdb: DuckDBResource):
    dropped = _drop_schemas(duckdb, context.log, (settings.RAW_SCHEMA,))
    context.add_output_metadata({"dropped_schemas": dropped})


@dg.op(
    description=(
        "Дропнуть производные схемы, snapshot train/holdout, эксперимент и registered model MLflow, "
        "dbt/target."
    )
)
def clean_derived(
    context: dg.OpExecutionContext,
    duckdb: DuckDBResource,
    mlflow_res: MlflowResource,
    paths: PathsResource,
):
    dropped = _drop_schemas(duckdb, context.log, DERIVED_SCHEMAS)

    ml_dir = Path(paths.ml_data_dir)
    parquet = list(ml_dir.glob("*.parquet")) if ml_dir.exists() else []
    for p in parquet:
        p.unlink()

    mlflow_res.setup()
    client = mlflow.MlflowClient()
    removed_model = False
    try:
        client.delete_registered_model(mlflow_res.model_name)
        removed_model = True
    except mlflow.MlflowException:
        pass
    exp = client.get_experiment_by_name(mlflow_res.experiment)
    removed_runs = 0
    if exp is not None:
        for run in client.search_runs([exp.experiment_id]):
            client.delete_run(run.info.run_id)
            removed_runs += 1
    artifacts = Path(mlflow_res.artifacts_dir)
    if artifacts.exists():
        shutil.rmtree(artifacts, ignore_errors=True)

    target = Path(settings.DBT_TARGET_PATH)
    for sub in ("run", "compiled", "run_results.json"):
        p = target / sub
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.is_file():
            p.unlink()

    context.add_output_metadata(
        {
            "dropped_schemas": dropped,
            "parquet_removed": len(parquet),
            "registered_model_removed": removed_model,
            "mlflow_runs_deleted": removed_runs,
        }
    )


@dg.job(description="Очистка raw: схема raw в DuckDB.")
def clean_raw_job():
    clean_raw()


@dg.job(
    description="Очистка всего производного: dbt-схемы, snapshot ML, MLflow (эксперимент + реестр)."
)
def clean_derived_job():
    clean_derived()


@dg.job(description="Полная очистка: raw + derived.")
def clean_all_job():
    clean_derived()
    clean_raw()


@dg.definitions
def maintenance() -> dg.Definitions:
    return dg.Definitions(jobs=[clean_raw_job, clean_derived_job, clean_all_job])
