# Прогресс реализации

Формат: `- [x] <шаг> — <commit> — <дата> — <что проверено>`. Шаги — [`../.claude/TODO.md`](../.claude/TODO.md).

## MVP

- [ ] M2 dbt как ассеты и checks — ветка `feat/m2-dbt-assets-checks` — 2026-09-21 —
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
