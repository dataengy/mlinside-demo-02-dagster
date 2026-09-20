# Прогресс реализации

Формат: `- [x] <шаг> — <commit> — <дата> — <что проверено>`. Шаги — [`../.claude/TODO.md`](../.claude/TODO.md).

## MVP

- [ ] M1 dbt-копия + shipped snapshot + `demo-prepare` — ветка `feat/m1-dbt-snapshot` — 2026-09-21 —
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
