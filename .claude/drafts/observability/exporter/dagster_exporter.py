#!/usr/bin/env python3
"""Sidecar-экспортер: Dagster GraphQL + MLflow REST -> метрики Prometheus.

Почему так (см. decisions.md, ADR-1): у Dagster нет ни OTel-экспорта, ни
/metrics; единственный стабильный публичный интерфейс — GraphQL webserver'а.
Один маленький процесс опрашивает его и MLflow и отдаёт /metrics — Prometheus
скрейпит только этот адрес.

Все поля GraphQL сверены по dagster-graphql==1.13.23 (исходники схемы):
  runsOrError(filter: RunsFilter, limit)      -> Runs.results[] {runId jobName status startTime endTime}
  assetNodes                                  -> AssetNode {assetKey, freshnessStatusInfo, assetChecksOrError, assetMaterializations}
  AssetCheck.executionForLatestMaterialization -> AssetCheckExecution {status}
Единицы: Run.startTime/endTime — СЕКУНДЫ, MaterializationEvent.timestamp —
МИЛЛИСЕКУНДЫ (строкой). Отсюда деление на 1000 ниже.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx
from prometheus_client import Counter, Gauge, start_http_server

# --- Конфигурация: только окружение, никаких скаляров в коде ниже. ---
GRAPHQL_URL = os.environ.get("DAGSTER_GRAPHQL_URL", "http://dagster-webserver:3111/graphql")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5001")
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "olist_ml")
MLFLOW_METRICS = [m.strip() for m in os.environ.get("MLFLOW_METRICS", "roc_auc").split(",") if m.strip()]
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "15"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9101"))
RUNS_WINDOW_SECONDS = float(os.environ.get("RUNS_WINDOW_SECONDS", "3600"))
RUNS_LIMIT = int(os.environ.get("RUNS_LIMIT", "200"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "10"))

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dagster_exporter")

# --- Метрики. Все Gauge: экспортер каждый цикл переписывает АБСОЛЮТНОЕ состояние,
#     монотонных счётчиков у источника нет (перезапуск экспортера ничего не ломает). ---
RUNS = Gauge("dagster_runs_total", "Запусков за окно RUNS_WINDOW_SECONDS", ["job", "status"])
RUN_DURATION = Gauge("dagster_run_duration_seconds", "Длительность последнего завершённого запуска джобы", ["job"])
CHECK_STATUS = Gauge("dagster_asset_check_last_status", "Последний asset check: 1=SUCCEEDED, 0=иначе", ["asset", "check"])
ASSET_MAT_TS = Gauge("dagster_asset_last_materialization_timestamp", "Unix-время последней материализации, сек", ["asset"])
FRESHNESS = Gauge(
    "dagster_asset_freshness_status",
    "FreshnessPolicy: 0=HEALTHY, 1=WARNING, 2=DEGRADED, -1=UNKNOWN, -2=NOT_APPLICABLE",
    ["asset"],
)
MODEL_METRIC = Gauge("mlflow_model_metric", "Метрика последнего run эксперимента MLflow", ["model", "metric"])
SCRAPE_ERRORS = Counter("dagster_exporter_scrape_errors_total", "Ошибки опроса источника", ["source"])

# UNKNOWN намеренно НИЖЕ нуля: правило алерта `== 2` не должно ловить «ещё не оценено».
FRESHNESS_CODE = {"HEALTHY": 0, "WARNING": 1, "DEGRADED": 2, "UNKNOWN": -1, "NOT_APPLICABLE": -2}

RUNS_QUERY = """
query ExporterRuns($filter: RunsFilter!, $limit: Int!) {
  runsOrError(filter: $filter, limit: $limit) {
    __typename
    ... on Runs { results { runId jobName status startTime endTime } }
    ... on PythonError { message }
  }
}
"""

ASSETS_QUERY = """
query ExporterAssetNodes {
  assetNodes {
    assetKey { path }
    freshnessStatusInfo { freshnessStatus }
    assetMaterializations(limit: 1) { timestamp }
    assetChecksOrError {
      __typename
      ... on AssetChecks {
        checks { name executionForLatestMaterialization { status } }
      }
    }
  }
}
"""


def graphql(client: httpx.Client, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Один POST в GraphQL. Ошибки транспорта и GraphQL-ошибки одинаково -> исключение."""
    response = client.post(GRAPHQL_URL, json={"query": query, "variables": variables or {}})
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(str(payload["errors"])[:300])
    return payload["data"]


def collect_runs(client: httpx.Client) -> None:
    """Счётчики запусков по статусам и длительность последнего завершённого — по окну."""
    variables = {"filter": {"updatedAfter": time.time() - RUNS_WINDOW_SECONDS}, "limit": RUNS_LIMIT}
    runs_or_error = graphql(client, RUNS_QUERY, variables)["runsOrError"]
    if runs_or_error["__typename"] != "Runs":
        raise RuntimeError(runs_or_error.get("message", runs_or_error["__typename"])[:300])

    counts: dict[tuple[str, str], int] = {}
    seen_jobs: set[str] = set()
    # Dagster отдаёт запуски от новых к старым, поэтому длительность берём у
    # ПЕРВОГО завершённого запуска джобы и дальше её для этой джобы не трогаем.
    for run in runs_or_error["results"]:
        job = run["jobName"]
        counts[(job, run["status"])] = counts.get((job, run["status"]), 0) + 1
        start, end = run.get("startTime"), run.get("endTime")
        if start and end and job not in seen_jobs:  # startTime/endTime — секунды
            RUN_DURATION.labels(job=job).set(max(end - start, 0.0))
            seen_jobs.add(job)
    RUNS.clear()  # окно скользящее: старые серии должны исчезать, а не «залипать»
    for (job, status), count in counts.items():
        RUNS.labels(job=job, status=status).set(count)


def collect_assets(client: httpx.Client) -> None:
    """Свежесть, последняя материализация и статус последнего прогона каждого check."""
    CHECK_STATUS.clear()
    for node in graphql(client, ASSETS_QUERY)["assetNodes"]:
        asset = "/".join(node["assetKey"]["path"])

        info = node.get("freshnessStatusInfo")
        if info:  # None, если у ассета нет FreshnessPolicy
            FRESHNESS.labels(asset=asset).set(FRESHNESS_CODE.get(info["freshnessStatus"], -1))

        materializations = node.get("assetMaterializations") or []
        if materializations:  # timestamp приходит строкой в МИЛЛИсекундах
            ASSET_MAT_TS.labels(asset=asset).set(float(materializations[0]["timestamp"]) / 1000.0)

        checks_or_error = node.get("assetChecksOrError") or {}
        if checks_or_error.get("__typename") != "AssetChecks":
            continue
        for check in checks_or_error["checks"]:
            execution = check.get("executionForLatestMaterialization")
            status = (execution or {}).get("status")
            CHECK_STATUS.labels(asset=asset, check=check["name"]).set(1.0 if status == "SUCCEEDED" else 0.0)


def collect_mlflow(client: httpx.Client) -> None:
    """Метрики последнего run эксперимента: REST 2.0 (пути сверены по mlflow 3.16.1)."""
    experiment = client.get(
        f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/experiments/get-by-name",
        params={"experiment_name": MLFLOW_EXPERIMENT},
    )
    if experiment.status_code == 404:  # эксперимента ещё нет — это не ошибка
        return
    experiment.raise_for_status()
    experiment_id = experiment.json()["experiment"]["experiment_id"]

    search = client.post(
        f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/runs/search",
        json={
            "experiment_ids": [experiment_id],
            "max_results": 1,
            "order_by": ["attributes.start_time DESC"],
        },
    )
    search.raise_for_status()
    runs = search.json().get("runs") or []
    if not runs:
        return
    metrics = {m["key"]: m["value"] for m in runs[0].get("data", {}).get("metrics", [])}
    for name in MLFLOW_METRICS:
        if name in metrics:
            MODEL_METRIC.labels(model=MLFLOW_EXPERIMENT, metric=name).set(metrics[name])


def main() -> None:
    log.info("exporter: :%s, dagster=%s, mlflow=%s, poll=%ss", EXPORTER_PORT, GRAPHQL_URL, MLFLOW_TRACKING_URI, POLL_SECONDS)
    start_http_server(EXPORTER_PORT)
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        while True:
            # Источники независимы: падение одного не должно гасить остальные метрики.
            for source, collect in (("dagster_runs", collect_runs), ("dagster_assets", collect_assets), ("mlflow", collect_mlflow)):
                try:
                    collect(client)
                except Exception as exc:  # noqa: BLE001 — сеть и чужие API падают как угодно
                    SCRAPE_ERRORS.labels(source=source).inc()
                    log.warning("%s: %s: %s", source, type(exc).__name__, str(exc)[:200])
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
