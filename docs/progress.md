# Прогресс реализации

Формат: `- [x] <шаг> — <commit> — <дата> — <что проверено>`. Шаги — [`../.claude/TODO.md`](../.claude/TODO.md).

## MVP

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
