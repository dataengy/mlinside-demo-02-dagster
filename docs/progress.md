# Прогресс реализации

Формат: `- [x] <шаг> — <commit> — <дата> — <что проверено>`. Шаги — [`../.claude/TODO.md`](../.claude/TODO.md).

## MVP

- [ ] M7b Второй сенсор — ветка `feat/m7-telegram-alert` — 2026-09-21 — `alert_on_failed_check`: курсор по зелёным
  run'ам (`get_run_records(updated_after)` + `all_logs(ASSET_CHECK_EVALUATION)`), алерт на каждый `passed=False`; run
  FAILURE пропускается (о нём говорит первый сенсор, дублей нет). Сцена: `just demo-break-warn` (срок доставки в часах
  вместо дней) + warn-тест `accepted_range(estimated_delivery_span_days ≤ 180, severity: warn)` в overlay → `dq_job`
  SUCCESS, check WARN, сообщение приходит. Тесты — на `DagsterInstance.local_temp` (настоящий SQLite): первая версия
  с `get_event_records(after_cursor=int)` падала на run-sharded event log `dg dev` («cursor is not run-aware») и
  ephemeral-инстанс этого не ловил. Попутно: относительные `DBT_*` из `.env` давали `dbt/dbt/target` — Justfile
  экспортирует абсолютные, `defs/env.py` абсолютизирует при загрузке.
- [ ] M8 Финал — ветка `feat/m7-telegram-alert` — 2026-09-21 — хронометраж демо-команд (машинное время, snapshot
  4 918 заказов, M2 MacBook): `demo-prepare` 29 с, `feature-mart` 14–17 с, `dq` 17–19 с, `train` 16 с, `promote` 10 с,
  `score` 9–10 с, `demo-recompute` 17 с, `demo-break/fix/sql-change` 2–5 с; вся последовательность S1–S18
  (17 команд) — 203 с ≈ 3,5 мин машинного времени, остальное из 30 мин — речь и UI. `just test-all`: 46 unit/smoke/
  integration + 1 e2e зелёные (первый e2e-прогон упал из-за гонки с параллельным `demo-break` по общим SQL-файлам —
  e2e и демо-команды нельзя запускать одновременно). Все `just`-рецепты из README/DEMO/runbook существуют.
  Починено: относительный `DAGSTER_HOME` в `.env` ломал `dg launch`/`dg dev` (dagster CLI грузит `.env` из cwd поверх
  окружения) — поле убрано из `Settings`/`.env.example`, владелец — Justfile; строка в runbook.
- [ ] M7 Telegram-алерт — ветка `feat/m7-telegram-alert` — 2026-09-21 — `alerts/telegram.py` (`format_run_failure`,
  `send_telegram` одним `httpx.post`, dry-run без `ALERTS_ENABLED`/`TG_*`), `defs/automation/sensors.py`:
  `@dg.run_failure_sensor alert_on_run_failure` (все джобы, `default_status=RUNNING`, 30 с), `just tg-test [text]`;
  5 unit-тестов на `httpx.MockTransport` (dry-run, POST в `sendMessage`, HTTP-ошибка, сенсор в defs);
  `just check` ✓, `just test` 45 passed. В UI: `demo-break → feature-mart → dq` (FAILURE) → тик сенсора SUCCESS с
  dry-run текстом (job, ссылка на run, первая строка ошибки). Подводный камень: без `monitor_all_code_locations=True`
  run'ы `dg launch` (нет `remote_job_origin`) пропускаются молча. Живая доставка не проверена: в `.env` нет
  `TG_BOT_TOKEN`/`TG_CHAT_ID`.
  CI PR #17 был красный (`actions/checkout lfs: true` → «failed to fetch some objects from …/info/lfs»):
  LFS-объекты не ушли на GitHub при push через inline credential helper — догружены `git lfs push --all origin`,
  job перезапущен.
- [ ] M6 CI — ветка `feat/m6-ci` — 2026-09-21 — `.github/workflows/ci.yml`: job `ci` = `just install` (сеть) →
  `just check` → `just test` (офлайн), job `e2e` (`just test-e2e`, не на PR); `checkout lfs: true` для snapshot;
  `extractions/setup-just@v3`, `astral-sh/setup-uv@v10.1.0`; без секретов. Новый `tests/e2e/test_demo_flow.py`
  (весь сюжет одним тестом). Бейдж в README. Зелёный run — после push ветки (см. PR).
- [x] M5 Выборочный пересчёт, freshness, очистка — `2b50562` + `c668b4c` (PR #16) — 2026-09-21 —
  живьём подтверждено: после `just demo-sql-change` + reload только `mart_order_features` → `STALE`
  (`CODE: has a new code version`), ML-ассеты `FRESH`; `just demo-recompute` (`dg launch --assets …`) выполнил
  `dbt_feature_branch → training_dataset → model → model_evaluation → quality_gate → model_registered`, raw/staging
  не запускались; `FreshnessPolicy.time_window` на витрине через `template_vars_module` (`mart_freshness`),
  статус `HEALTHY`; `defs/maintenance/jobs.py`: `clean_raw|derived|all_job` (@op/@job), тест: пустое состояние ок,
  уровни, `clean_all → demo_prepare` воспроизводит counts и версию 1.
- [x] M4 Promotion + batch inference — `6371ea4` (PR #15) — 2026-09-21 —
  `defs/promotion.py` (`promote_job` → alias champion на версию/последнюю, `demote_job`), `demo_prepare_job`
  = raw → витрина + checks → baseline-версия без алиаса (`role=baseline`, сид 7; ADR-07a A),
  `defs/ml/inference.py`: `scoring_input` (строки с target NULL → `ml.scoring_input`) → `predictions`
  (alias → версия один раз, полный Pipeline, `ml.predictions` с `model_version`/`batch_id`, delete+insert по
  batch, `dg.Failure` «выполните promote» без champion), `score_job`; `--indirect-selection cautious` у dbt-компонента
  (eager тянул тесты канона на модели вне графа). Тест: baseline → fail без champion → promote → v1 → без
  дублей → новый кандидат не меняет champion → promote v2 → demote.
- [x] M3 ML контур A — `2477a2c` (PR #14) — 2026-09-21 —
  `ml/features.py` (контракт 10 признаков, хеш-сплит, LogReg Pipeline, fingerprint), ассеты
  `training_dataset` (snapshot train/holdout parquet) → `model` (MLflow run) → `model_evaluation` (holdout) →
  `quality_gate` (blocking, `GateConfig.min_roc_auc`) → `model_registered` (версия без алиаса, идемпотентно по
  fingerprint); `train_job` = ML ∪ blocking dbt-checks витрины (ADR-04a A) + `AutomationCondition.eager() &
  all_deps_blocking_checks_passed()` на `training_dataset` (B); порог измерен `scripts/measure_gate.py`:
  0.629–0.689 → `ML_MIN_ROC_AUC=0.61`. Тесты: unit features, integration train_job (версия 1 без алиаса,
  повтор → `reused_existing`, gate 0.99 → registered не запущен, сломанный контракт → ML пропущена).
- [x] M2 dbt как ассеты и checks — `4240937` (PR #13) — 2026-09-21 —
  `defs/dbt/defs.yaml` (`select +mart_order_features`, `exclude path:seeds/raw`, группы по слою, owners/tags;
  `op.name` у обоих компонентов — иначе граф не сшивается), `mart_order_features.sql` + yml (все заказы,
  target NULL у недоставленных; тесты unique/not_null/accepted_range → blocking checks),
  `feature_mart_job` / `dq_job` / `dbt_build_job`, `scripts/demo_patch.py` (`just demo-break|fix|sql-change`);
  тесты: граф связный raw→mart, feature_mart без checks, dq без материализаций, идемпотентность,
  негативный сценарий «модель зелёная, unique(order_id) красный».
- [x] M1 dbt-копия + shipped snapshot + `demo-prepare` — `afe9666` (PR #12) — 2026-09-21 —
  `just dbt-sync` (rsync канона + overlay: profiles duck, sources.yml с `meta.dagster.asset_key`, схемы без
  префикса, `month_of`), `just seeds-sample` → 8 CSV (4 918 заказов, 5.5 МБ, git-lfs) + фикстуры (496),
  `defs/ingest/defs.yaml` (`DbtProjectComponent`, `select: path:seeds/raw`, ключи `raw/<table>`),
  `demo_prepare_job`; ADR-04b `prepare_if_dev: false` (dbt deps ходит в hub при каждой загрузке);
  `just check` ok, `just test` 21 passed (unit sampling, smoke contract, integration seed×2 = те же counts),
  `just dev` → 8 ассетов группы `raw`, `just demo-prepare` → `raw.*` в DuckDB.
- [x] M0 Scaffold + settings + Justfile + smoke — `5116ea1` (PR #11, ветка `feat/m0-scaffold`) — 2026-09-20 —
  `uvx create-dagster@1.13.23 project olist_ml` (сверено во временной папке: `pyproject` с `[tool.dg]`,
  `definitions.py` = `load_from_defs_folder`), `uv sync` (dagster 1.13.23, dagster-dbt 0.29.23, mlflow 3.16.1,
  sklearn 1.9.1, dbt 1.12.5), `just check` (dg check defs + ruff), `just test` 13 passed, `just dev` → UI
  http://127.0.0.1:3000, location `olist_ml` загружен, 0 ассетов / 0 джоб.
