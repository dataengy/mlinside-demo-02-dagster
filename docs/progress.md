# Прогресс реализации

Формат: `- [x] <шаг> — <commit> — <дата> — <что проверено>`. Шаги — [`../.claude/TODO.md`](../.claude/TODO.md).

## MVP

- [ ] M0 Scaffold + settings + Justfile + smoke — (в работе, ветка `feat/m0-scaffold`) — 2026-09-20 —
  `uvx create-dagster@1.13.23 project olist_ml` (сверено во временной папке: `pyproject` с `[tool.dg]`,
  `definitions.py` = `load_from_defs_folder`), `uv sync` (dagster 1.13.23, dagster-dbt 0.29.23, mlflow 3.16.1,
  sklearn 1.9.1, dbt 1.12.5), `just check` (dg check defs + ruff), `just test` 13 passed, `just dev` → UI
  http://127.0.0.1:3000, location `olist_ml` загружен, 0 ассетов / 0 джоб.
