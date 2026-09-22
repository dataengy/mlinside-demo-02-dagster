# TODO — mlinside-demo-02-dagster

Пошаговый план (ревизия v3, 2026-09-20). Решения — [`docs/decisions.md`](../docs/decisions.md)
(ADR-01…ADR-18), целевое состояние и appendix — [`docs/overview.md`](../docs/overview.md), сценарий показа —
[`demo/`](../demo/README.md), контракты — [`docs/contracts/`](../docs/contracts/). Архив прежнего плана —
[`.archive/`](.archive/) (факты и разведка, не требования).

**Правила для каждого шага:** минимальное изменение, проверяемое в GUI Dagster → `just check && just test`
зелёные → отчёт пользователю: что и где посмотреть + 1–3 вопроса от простого к сложному → строка в
`docs/progress.md` → коммит Conventional Commits; push — после подтверждения. Шаги строго по порядку.
Всё, что помечено «⚠ проверить», сначала подтверждается на установленных версиях (dagster 1.13.23,
dagster-dbt 0.29.23, mlflow 3.15) и только потом попадает в DEMO.

## MVP

- [ ] M0 Scaffold `create-dagster project olist_ml` (⚠ проверить команду scaffold и `defs.yaml` компонента), `settings.py` + `.env.example`, `Justfile` из черновика Makefile, `dagster.yaml` (`freshness.enabled`), smoke `dg check defs` — ADR-02 ADR-09 ADR-15 #scaffold #mvp (#2)
- [ ] M1 dbt-копия канона + shipped snapshot: `sync_dbt_from_canonical.sh`, `profiles.yml`, макросы, `sources.yml` с `meta.dagster.asset_key`, seeds `raw/*` (~5 000 заказов, git-lfs), `defs/ingest/defs.yaml` (seeds → `raw/*`), `just demo-prepare`, `docs/contracts/raw.md` + smoke-тест контракта — ADR-03 ADR-05 ADR-14 #dbt #ingest #mvp (#3)
- [ ] M2 dbt как ассеты и checks: `defs/dbt/defs.yaml` (`select +mart_order_features`, owners/tags), `mart_order_features.sql` + yml, `feature_mart_job` / `dq_job` раздельно, ⚠ проверить blocking провала dbt-теста для ML-ветки, негативный e2e «сломанный контракт → check failed → ML не выполнена» — ADR-04 ADR-05 #dbt #mvp (#4)
- [ ] M3 ML контур A: `docs/contracts/features.md`, `training_dataset` (snapshot train/holdout), `model` (LogReg Pipeline, 8–10 признаков), `model_evaluation`, `quality_gate` (порог измерить по 5 сидам), `model_registered` без алиаса, `MlflowResource`, `train_job` — ADR-06 ADR-07 ADR-14 #ml #mvp (#5)
- [ ] M4 Promotion и batch inference: `promote_job` (алиас `champion`), baseline-версия в `demo-prepare`, `scoring_input` → `predictions` (batch_id, model_version, идемпотентно, понятный fail без champion), `score_job` — ADR-07 ADR-13 #ml #mvp (#6)
- [ ] M5 Выборочный пересчёт и freshness: ⚠ проверить статусы UI после правки SQL витрины (default `code_version`), `FreshnessPolicy.time_window` на `mart_order_features`, заготовленные патчи для DEMO (`demo-break`/`demo-fix`), очистка `clean raw|derived|all` + тесты идемпотентности — ADR-10 ADR-17 #dagster #mvp (#7)
- [ ] M6 CI GitHub Actions: `ci.yml` = `just install / check / test`, e2e отдельным job, без сети и секретов — ADR-11 #ci #mvp (#8)
- [ ] M7 Алерт: `run_failure_sensor` → Telegram (`httpx`, dry-run, unit-тест на мок), проверка живого сообщения — ADR-18 #observability #mvp (#9) — код + dry-run в UI готовы (`feat/m7-telegram-alert`); live-доставка ждёт `TG_BOT_TOKEN`/`TG_CHAT_ID` в `.env`
- [ ] M8 Финал: [`demo/`](../demo/README.md) прогнан по таймингу ≤30 мин, `docs/runbook.md` сверен с реальными командами, `just test-all` зелёный, отчёт в `docs/progress.md` #docs #tests #mvp (#10) — машинный хронометраж 3,5 мин, test-all зелёный, рецепты сверены; остался живой прогон по речи

## Appendix / после лекции

- [ ] P1 dlt из Kaggle как stretch по критериям ADR-03 (`.claude/drafts/ingest/`), паритет контракта raw на двух загрузчиках — #ingest
- [ ] P2 Evidently HTML-отчёт по `predictions` как расширенное демо — ADR-16 #observability
- [ ] P3 Docker Compose core + Grafana-стек + экспортер (`.claude/drafts/observability/`), `docs/observability.md` — ADR-12 #docker #observability
- [ ] P4 Партиции `predictions`, backfill, `SIM_TODAY`, `prediction_monitoring`, поздние метки — [overview §6](../docs/overview.md) #ml
- [ ] P5 `TrainConfig.split = hash|time`, challenger/champion сравнение на одном holdout — overview §6.2 #ml
- [ ] P6 `dbt source freshness` как asset checks; `INTEGRATIONS_STYLE=python`, `integrations_python/` — архив D1 D4 #dbt
- [ ] P7 `docs/deploy/`: Hetzner VM и Dagster+ (Serverless/Hybrid), сравнительная таблица, mermaid — #docs
- [ ] P8 Agentic BRD-watch: хук, скилл, субагент из `.claude/drafts/agentic/` — архив D13 #agentic
- [ ] P9 Agentic backlog: разобрать черновики `.claude/.tmp/` (`md_label_blanklines.py`, `md_hardbreak.py`, `Justfile`, `README-revision.md`) и перенести/отрефакторить — скрипты → `scripts/utils/` (+ рецепты в корневой Justfile), правила разметки → skill (`/create-skill-candidate`), автопроверка `.md` → PostToolUse-hook, настройки → `config/config.yml:docs.markdown.refining`, память → `markdown-refining-style` — #agentic #docs
- [ ] P10 Security: токен бота `@dagster_demo_bot` попал в текст чата (сессия 2026-09-21) — после демо перевыпустить через BotFather (`/revoke`), обновить `.env`; в репо токен не идёт (`.env` в `.gitignore`), в `.env.example` — только пустой `TG_BOT_TOKEN=` #security #observability
- [ ] P11 Non-critical находки codex-ревью PR #19: [#20](https://github.com/dataengy/mlinside-demo-02-dagster/issues/20) испорченная разметка в `docs/decisions.md` (лишние двойные бэктики), [#21](https://github.com/dataengy/mlinside-demo-02-dagster/issues/21) `config/config.yml` ссылается на игнорируемый `.claude/.tmp/Justfile` — оба пересекаются с P9 #docs
