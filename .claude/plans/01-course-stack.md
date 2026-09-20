# Инструменты курса: обучение, прод, мониторинг, observability
original (this's hardlinked): ~/.claude/plans/sharded-chasing-hartmanis.md

## Context
Вопрос пользователя: какими инструментами курс обеспечивает обучение моделей, работу в проде,
мониторинг (в обучении и в проде) и observability. Источники:
- первоначальный план: `settings/schedule.yml` (лист mlinside-mlops, 8 лекций, дамп 2026-08-19);
- таблица ВШЭ «Проектирование систем ML…», вкладка «Тематический план» (28 занятий, 10.09–17.12.2026);
- финальные деки Влада: `data/@Vlad's-presentations/` (1, 2, 3, 5, 7);
- для лекции 11 — наша дека `content/preza-cicd-observability-content.yml`.

Код не меняется; это аналитический ответ.

## Сводка
| Область | Инструменты | Где |
|---|---|---|
| Обучение | LightGBM, CatBoost, sklearn, Optuna; DVC + MinIO; Feast offline (ClickHouse/Parquet); MLflow Tracking/PyFunc/Registry (Postgres + S3); переобучение Airflow/Dagster; SageMaker/Vertex/AzureML операторы | деки 7, 5; занятия 6, 8, 10 |
| Прод | FastAPI + Pydantic + uvicorn; Docker, multi-stage, Compose, volumes; MLflow `@champion/@challenger`; Feast online (Redis); Kafka; batch-скоринг в Airflow; CI GitLab/GitHub Actions; LLM: vLLM, Triton, llama.cpp, TEI, K8s | деки 1, 2, 5, 7; занятия 11, 19–24 |
| Мониторинг обучения | MLflow метрики и кривые, `dvc metrics/plots diff`, UI Airflow/Dagster, dbt tests | деки 7, 5 |
| Мониторинг в проде | логи инференса → Kafka → ClickHouse (Buffer/Null + MV) → Grafana; drift-проверки в DAG после скоринга; Evidently только в обзорной таблице | деки 2, 3, 5; занятие 11 |
| Observability | Prometheus, PromQL, Grafana, Alertmanager → Telegram/Mattermost, SLI/SLO, OpenTelemetry, Loki/Tempo/Jaeger; ClickHouse system.*; Airflow retries/алерты, OpenLineage; LLM: DCGM, genai-perf, Langfuse/Phoenix | занятие 11, деки 3, 5; занятия 23–24 |

## Пробелы
1. Нет практического инструмента drift-мониторинга; у Влада drift = SQL по ClickHouse, Evidently лишь упомянут.
2. Serving классического ML только FastAPI; BentoML/KServe/Seldon нет.
3. Занятие 17 «…мониторинг качества моделей» без тезисов; тезисы 15–17 сдвинуты на строку.
4. Занятие 10 в плане «MLflow / ClearML», фактически дека 7 «DVC, Feast, MLflow»; ClearML только в обзоре.
5. Лектор занятия 11 разошёлся: в старом листе «Николай/Влад», в ВШЭ-таблице Малыхин.

## Verification
Сверено по тексту PDF (pypdf), CSV-экспорту всех вкладок таблицы и YAML контента деки.
