# План: MVP `mlinside-demo-02-dagster` — документы, TODO, gh-issues
orig: /Users/user/.claude/plans/enchanted-pondering-phoenix.md

## Context

Репо `dataengy/mlinside-demo-02-dagster` пока содержит только `.claude/PLAN.md` (v2, полный проект на 4 демо), `.claude/PROMPT.md` (ТЗ) и черновики лейнов-разведки в `.claude/drafts/`. Пользователь хочет **сначала MVP**: минимум моделей/витрин для сквозного демо ingest → dbt → ML, минимальный ML-сэмпл, идемпотентная загрузка, Makefile с полной очисткой в трёх вариантах. Эта задача — **не код, а документы и планирование**: ADR-решения для первой реализации, пошаговый TODO с gh-issues, README, архитектура, runbook, DEMO-шпаргалка. Код пишется следующими сессиями по TODO.

Решения пользователя (AskUserQuestion, 2026-09-20): ingest MVP = **dlt из Kaggle** (kagglehub анонимно → сэмпл по `order_id` → DuckDB `raw`); в MVP входят **CI (GitHub Actions)** и **Docker Compose core** (Dagster+MLflow+Postgres, без Grafana); раздельные `transform_job`/`dq_job`, freshness, observability, dbt-seed-вариант — **вне MVP** (в TODO как «после MVP»).

## Scope MVP (что фиксируется в decisions.md)

| Слой | MVP | Отложено |
|---|---|---|
| Ingest | `DltLoadCollectionComponent` (`defs/ingest/defs.yaml` + `loads.py`, `kaggle_olist.py`, `sampling.py` из `.claude/drafts/ingest/`), ключи `raw/<t>` для 8 таблиц, `write_disposition="replace"`; CSV кладутся в `data/raw/` (из kagglehub cache), тесты — фикстуры `tests/fixtures/*.csv` через тот же source (`source_dir` параметр) | dbt seed v1, `INGEST_MODE` |
| dbt | копия канона (`scripts/sync_dbt_from_canonical.sh`), `DbtProjectComponent` с `select: "+mart_order_features"` → 8 stg + 3 int + 1 mart (+2 справочных seed); `sources.yml` с `meta.dagster.asset_key`, макросы `generate_schema_name`, `month_of`; `dbt_build_job` | 5 остальных marts (остаются в копии, не в графе), `transform_job`/`dq_job`, freshness |
| ML | `training_dataset → model → model_evaluation → model_registered`, HGB, `quality_gate` blocking, `MlflowResource` на SQLite; сэмпл = `SAMPLE_FRAC` (MVP 0.05 ≈ 5 тыс. заказов), гейт `ML_MIN_ROC_AUC` калибруется на сэмпле (стартовое 0.60, пересчитать по 5 сидам в задаче ML) | PNG-метаданные оставить, permutation importance — опц.; сенсоры/AutomationCondition |
| Jobs | `ingest_job`, `dbt_build_job`, `ml_job`, `full_pipeline_job` (+ schedule STOPPED) | — |
| Maintenance | `defs/maintenance/jobs.py`: `clean_raw_job`, `clean_derived_job` (dbt-схемы + MLflow + target), `clean_all_job`; Makefile: `clean-raw` / `clean-derived` / `clean-all` (+ `clean` = кеши сборки) | отдельные clean_dbt/clean_ml по этапам |
| Тесты | smoke (`dg check defs`, граф связный), unit (sampling, features, gate, settings↔.env.example), integration (ingest→tmp DuckDB на фикстурах ×2, dbt ×2, ml ×2 = идемпотентность), e2e (`clean_all → full_pipeline → повтор`) | style parity, telegram |
| CI | `.github/workflows/ci.yml` из `.claude/drafts/ci/` — `make install/check/test`, e2e на main | — |
| Docker | `Dockerfile`, `docker-compose.yml` профиль `core` (webserver, daemon, postgres, mlflow) из `.claude/drafts/observability/docker-compose.yml` без observability-сервисов; `deploy/dagster.prod.yaml` | профиль `observability`, Grafana, Telegram |

Идемпотентность: dlt replace; dbt `table`; ML — `dataset_fingerprint` тег → `reused_existing`; каждая clean-джоба — успех на пустом состоянии.

## Файлы, которые создаёт эта задача

1. **`docs/decisions.md`** — ADR-01…ADR-12 (по 5–10 строк: контекст/решение/последствия), на основе PLAN D1–D14 в сокращении под MVP: 
   ADR-01 отдельный репо (D14); ADR-02 scaffold-раскладка, YAML-компоненты, python-fallback отложен (D1); ADR-03 ingest = dlt из Kaggle анонимно + сэмпл по order_id, `data/raw` как кеш (D11, решение пользователя); ADR-04 копия dbt + `select +mart_order_features` (D2/D9); ADR-05 ключи `raw/<t>` через `meta.dagster.asset_key`; ADR-06 ML-задача, витрина без утечек, хеш-сплит, гейт от минимума по сидам на сэмпле (D6); ADR-07 свой `MlflowResource`, fingerprint-идемпотентность (D5); ADR-08 DuckDB + `in_process_executor` (D7); ADR-09 settings-only скаляры, плоский `.env.example` (⚠️ отступление от глобального `config/*.template` — фиксируем); ADR-10 три уровня очистки `raw`/`derived`/`all`; ADR-11 CI = Make-цели; ADR-12 Compose core на Postgres (D8), observability отложена; ADR-13 training vs inference: MVP = только контур A (обучение); контракт с контуром B — алиас `@champion` в реестре MLflow; `predictions` + `SIM_TODAY` (лейн L-H, PLAN §6) и мониторинг — после MVP; подробности — `docs/overview.md` §6 (не дублировать, ссылаться).

Учёт уже существующих документов (коммиты 9f95f5c, 049ff69): `docs/overview.md` — **не трогать**, это «целевое описание»; README/architecture ссылаются на него; в TODO «После MVP» — отдельная строка L-H (`predictions` daily partitions, `prediction_monitoring`, `SIM_TODAY`) со ссылкой на overview §6.5-сценарий. Ссылки на PLAN.md §/D для деталей.
2. **`.claude/TODO.md`** — секции `## MVP` (шаги M0–M9 ниже, каждая строка `- [ ] текст #label`, формат под `sync-md.sh`) и `## После MVP` (dbt seed v1, transform/dq+freshness, observability+Telegram, docs/deploy, agentic BRD-watch). Каждый шаг ссылается на ADR и на `.claude/tasks/ghi-*.md` для крупных.
   - M0 scaffold + settings + Makefile-ядро + smoke (`.claude/tasks/ghi-m0-scaffold.md`) `#scaffold`
   - M1 копия dbt + sources/macros + `mart_order_features` + `dbt build` локально (`ghi-m1-dbt-copy.md`) `#dbt`
   - M2 ingest dlt из Kaggle: source/sampling/component, `data/raw`, фикстуры, `ingest_job` (`ghi-m2-ingest-dlt.md`) `#ingest`
   - M3 `DbtProjectComponent`, связный граф, `dbt_build_job` `#dbt`
   - M4 ML-ассеты + MLflow + gate + `ml_job`/`full_pipeline_job`, калибровка порога на сэмпле (`ghi-m4-ml.md`) `#ml`
   - M5 maintenance clean_* + Makefile clean-raw/derived/all + тесты идемпотентности `#maintenance`
   - M6 CI GitHub Actions `#ci`
   - M7 Docker Compose core + Dockerfile + prod dagster.yaml (`ghi-m7-compose.md`) `#docker`
   - M8 e2e + `make test-all` зелёный + progress-отчёт `#tests`
   - M9 docs финал: runbook по факту, DEMO.md команды сверены, README-бейдж CI `#docs`
3. **`.claude/tasks/ghi-{m0,m1,m2,m4,m7}-*.md`** — детализация: файлы, интерфейсы, шаги TDD (перенос из PLAN §5 S1–S4, §6 L-A/L-C/L-E), критерий готовности.
4. **`README.md`** — переписать: что это, mermaid-схема ingest→dbt→ML, quick start (`make install && make data && make dev`), навигация (docs/, DEMO.md, .claude/), статус MVP, происхождение.
5. **`docs/architecture.md`** — компоненты (Dagster code location, dlt, dbt, DuckDB, MLflow), mermaid: граф ассетов, схема данных (`raw/staging/intermediate/marts` в DuckDB), джобы и очистка, конфигурация (settings/.env), режимы запуска (local / compose), тест-пирамида; таблица «файл → ответственность» (сокращённый PLAN §2 под MVP).
6. **`docs/runbook.md`** — операционный: установка, запуск, `make`-цели, очистка, типовые сбои (Kaggle недоступен, порт 5000/AirPlay, DuckDB lock, dbt deps), проверка здоровья (`dg check defs`, GraphQL). `docs/runbook-demo.md` — сценарии демо 1–3 с таймингом (скелет PLAN §8, урезанный под MVP).
7. **`DEMO.md`** — шпаргалка: по шагам «что планируем / какие исходники смотрим / bash-команда + `make`-дубль / что должно получиться + ссылка (UI/MLflow/файл)». Шаги: 0 подготовка (`make install`, `.env`), 1 ingest (Kaggle→raw, показать `defs/ingest/defs.yaml`, `sampling.py`), 2 dbt (граф, `dbt_build_job`, `mart_order_features.sql`), 3 ML (`ml_job`, метаданные, MLflow UI, сломать gate через `ML_MIN_ROC_AUC`), 4 полный прогон с нуля (`make clean-all && make full`), 5 CI/Compose (Actions, `make docker-up`).
8. **gh-issues**: после записи TODO — `just -f ~/.ai/integrations/github/Justfile issue-sync-dry .claude/TODO.md --section "## MVP"`, затем `issue-sync … --write-back` (пишет `(#N)` в строки). Предусловие: `gh auth status` сейчас *Timeout keyring* — проверить вне sandbox (`dangerouslyDisableSandbox`), при неуспехе попросить пользователя `gh auth refresh -h github.com` и оставить синк как последний шаг.
9. **`.claude/CLAUDE-curr-status.md`** и `.claude/.PROMPTS-LOG.md` — по глобальному CLAUDE.md (статус + лог промпта).

Не трогаю: `.claude/PLAN.md`, `.claude/PROMPT.md`, `.claude/drafts/`.

## Уже сделано (коммит `333f30c` на `main`)

- `docs/decisions.md` — ADR-01…ADR-13 (включая ADR-13 «обучение vs инференс»).
- `.claude/TODO.md` — секции `## MVP` (M0–M9) и `## После MVP` (P1–P10).
- Worktree `docs-mvp-plan` смержен в `main` (ff) и удалён; сессия работает в основном каталоге.
- Черновик памяти `~/.claude/projects/…/memory/no-worktree-when-no-competing-branches.md` создан — **переписать** под новое правило (см. ниже), индекс `MEMORY.md` ещё не создан.

## Рабочий процесс (по указанию пользователя, 2026-09-20)

GitHub-flow / trunk-based, **без worktree**, в текущем каталоге `~/gi/@dataengy/mlinside-demo-02-dagster`:
`git switch -c feat/mvp-docs` от `main` → мелкие коммиты Conventional Commits → push → PR → squash/merge в `main`.
Worktree заводить только при реальной параллельной работе над тем же репо. `main` защищаем: не коммитим
в него напрямую, не force-push, не merge без PR.

Коммит `333f30c` уже лежит в `main` (сделан до этого решения) — переносить не нужно, ветка `feat/mvp-docs`
стартует поверх него.

## Порядок выполнения

1. `git switch -c feat/mvp-docs` (в текущем каталоге, от `main` = `333f30c`).
2. Дописать документы: `.claude/tasks/ghi-*.md` → `docs/architecture.md` → `docs/runbook.md` + `docs/runbook-demo.md` → `README.md` → `DEMO.md` (ссылки согласовать: TODO→ADR, DEMO→runbook, README→всё).
3. Проверка: все относительные ссылки в md существуют, TODO-строки парсятся `issue-sync-dry`.
4. gh-issues sync (с обработкой auth-проблемы) → write-back `(#N)` в TODO.md.
5. Правила и память (отдельным коммитом, вне репо проекта — файлы в `~/.claude/`):
   - переписать `~/.claude/projects/-Users-user-gi--dataengy-mlinside-demo-02-dagster/memory/` →
     `feature-branch-github-flow.md` (type: feedback): «работать в `feat/*` в текущем каталоге по
     GitHub-flow/TBD; worktree — только при конкурирующих сессиях; не коммитить прямо в `main`»,
     + строка в `MEMORY.md`;
   - добавить тот же пункт в глобальные правила: новый `~/.claude/rules/git-workflow.md`
     (рядом с существующим `context7.md`) и короткую ссылку на него в разделе Core Workflow
     `~/.claude/CLAUDE.md`.
6. Push `feat/mvp-docs` → PR в `dataengy/mlinside-demo-02-dagster` (draft, если gh-auth починится; иначе отдать пользователю команду).

## Verification

- `just -f ~/.ai/integrations/github/Justfile issue-sync-dry .claude/TODO.md` показывает 10 MVP-строк с правильными labels.
- Скрипт проверки ссылок в md — 0 битых.
- После синка в TODO.md у каждого MVP-шага есть `(#N)`, `gh issue list -R dataengy/mlinside-demo-02-dagster` показывает их.
- `git log --oneline main..feat/mvp-docs` — только docs-коммиты; `git status` чист; `git worktree list` — одна запись.
- `~/.claude/rules/git-workflow.md` существует и упомянут в `~/.claude/CLAUDE.md`; память проекта содержит запись о feat-ветках.
- DEMO.md: каждая make-цель упомянута в TODO/architecture как обязательная к реализации (сверка списка целей: `install env data dev mlflow full demo1-3 test test-e2e test-all check clean clean-raw clean-derived clean-all docker-build docker-up docker-down`).
