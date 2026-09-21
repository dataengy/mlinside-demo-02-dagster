# Текущий статус — mlinside-demo-02-dagster

Обновлено: 2026-09-20 (сессия «mvp pipeline with minimal models», ветка `feat/mvp-plan-v3`).

## Последние 3 завершённые задачи

1. Ревизия v3 плана и сценария демо: `docs/decisions.md` (ADR-01…18), `.claude/TODO.md` (M0–M8, appendix),
   `docs/overview.md` (сюжет ≤30 мин, §6 → appendix), `README.md`, `docs/contracts/{raw,features}.md`,
   `DEMO.md`, `docs/runbook.md` — ветка `feat/mvp-plan-v3`, коммиты `3ffca30…`.
2. Read-only аудит по брифу «Ревизия и доработка плана» (состояние checkout, покрытие §4–§13, версии пакетов,
   что проверить на 1.13.23) — `~/.claude/plans/enchanted-pondering-phoenix.md`.
3. Первый MVP-план (dlt из Kaggle, 4 демо) — `333f30c`, затем пересмотрен ревизией v3; `PLAN.md`/`PROMPT.md`
   перенесены в `.claude/.archive/` (`a3641b5`).

Проход 2 (2026-09-20, вечер): порт 3000; `DEMO.md` → слайды (S0–S22), прежний план → `DEMO-plan.md`;
`docs/theory/training-in-demo.md` + кандидат скилла `demo-training-selfcheck`
(`~/.ai/skills/_skills_candidates_by_prj/mlinside-demo-02-dagster/`); ADR-04a и ADR-07a — варианты + SWOT +
quadrant, решения открыты. Push `feat/mvp-plan-v3`, draft PR
[#1](https://github.com/dataengy/mlinside-demo-02-dagster/pull/1), issues
[#2–#10](https://github.com/dataengy/mlinside-demo-02-dagster/issues) (M0–M8).

## Незавершённое (последние 2 рабочих дня)

- Решения ADR-04a (гейт ML по dbt-check) и ADR-07a (baseline к scoring) — за автором.
- Промоут кандидата скилла: `/create-skill demo-training-selfcheck`.
- Кода нет: шаг M0 (scaffold + settings + Justfile + smoke) — первый шаг реализации, по правилу
  «минимальное изменение → GUI → тесты → отчёт + 1–3 вопроса».
- Проверки на установленных версиях (см. runbook §1, пометки «⚠ проверить» в DEMO) — по мере M0–M5.

## Предлагаемые следующие шаги

1. Ревью PR #1 → merge в `main`; выбор по ADR-04a / ADR-07a.
2. M0: scaffold `create-dagster` во временной папке для проверки команды, затем в репо; `Justfile` из
   черновика Makefile; `settings.py`; `dg check defs` зелёный; показать пустой UI.
3. M1: копия dbt + snapshot seeds (git-lfs) + `demo-prepare`; контракт raw smoke-тестом.
