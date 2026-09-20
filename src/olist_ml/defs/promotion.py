"""Promotion — явное действие, отдельное от регистрации (ADR-07): алиас champion → версия реестра.

Единственное (кроме maintenance) место с @op/@job: promotion ничего не материализует, это операция над
реестром. Откат = перевод алиаса на прошлую версию; demote — снять алиас (S15: «нет опубликованной модели»).
"""

import contextlib

import dagster as dg
import mlflow

from olist_ml.resources import MlflowResource


class PromoteConfig(dg.Config):
    # None → последняя зарегистрированная версия (все версии в реестре прошли gate — иначе их бы не было).
    version: int | None = None


@dg.op(description="Перевести алиас champion на указанную (или последнюю) версию реестра.")
def promote_champion(
    context: dg.OpExecutionContext, mlflow_res: MlflowResource, config: PromoteConfig
):
    mlflow_res.setup()
    client = mlflow.MlflowClient()
    name, alias = mlflow_res.model_name, mlflow_res.model_alias
    versions = client.search_model_versions(f"name='{name}'")
    if not versions:
        raise dg.Failure(description=f"в реестре нет версий {name!r} — сначала train_job")
    target = config.version or max(int(v.version) for v in versions)
    previous = None
    with contextlib.suppress(mlflow.MlflowException):  # алиас ещё не установлен — это норма
        previous = int(client.get_model_version_by_alias(name, alias).version)
    client.set_registered_model_alias(name, alias, str(target))
    context.log.info("alias %s: %s → %s (%s)", alias, previous, target, mlflow_res.model_url())
    context.add_output_metadata(
        {"model_name": name, "alias": alias, "version": target, "previous_version": previous or 0}
    )


@dg.op(description="Снять алиас champion — после этого predictions завершается понятной ошибкой.")
def demote_champion(context: dg.OpExecutionContext, mlflow_res: MlflowResource):
    mlflow_res.setup()
    client = mlflow.MlflowClient()
    try:
        client.delete_registered_model_alias(mlflow_res.model_name, mlflow_res.model_alias)
        context.log.info("alias %s снят", mlflow_res.model_alias)
    except mlflow.MlflowException as e:
        context.log.warning("алиас уже отсутствует: %s", e)


@dg.job(description="Promotion: alias champion → версия (по умолчанию последняя).")
def promote_job():
    promote_champion()


@dg.job(description="Снять alias champion (демо S15 / откат вручную).")
def demote_job():
    demote_champion()


@dg.definitions
def promotion() -> dg.Definitions:
    return dg.Definitions(jobs=[promote_job, demote_job])
