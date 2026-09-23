# demo/TODO.md — задание для Claude Code

> Исполнитель — Claude Code в корне репозитория `mlinside-demo-02-dagster`.
> Цель: прогнать все тесты, пройти оба демо по шагам со всеми проверками, выверить и исправить URL Dagster UI,
> затем развивать `src/demo_prep` и агентную обвязку (skills / agents / hooks / memory / инструкции).
> Документы демо: [README](README.md); по демо N = 1…4 — [`{N}-1-prepare.md`](docs/1-1-prepare.md), [`{N}-2-run.md`](docs/1-2-run.md), [`{N}-3-results.md`](docs/1-3-results.md)
> (1 — Dagster с нуля, 2 — dbt, 3 — CI + Observability, 4 — ML). Проверки шагов — [`tests/steps.py`](tests/steps.py),
> рецепты — [`Justfile`](Justfile), [`steps.just`](steps.just).

## Навигация

- [0. Правила работы](#t0)
- [1. Окружение и порт 3000](#t1)
- [2. Все тесты](#t2)
- [3. Демо 1 по шагам](#t3)
- [4. Демо 2–4 по шагам](#t4)
- [5. Аудит и исправление URL Dagster UI](#t5)
- [6. Отчёт](#t6)
- [7. Разработка `src/demo_prep`: scripts / configs / settings](#t7)
- [8. Агентная обвязка: skills / agents / hooks / memory / инструкции](#t8)

---

<a id="t0"></a>
## 0. Правила работы

1. Язык документов и отчётов — русский; формат слайдов не меняется:
   - главный поинт → «Дано / на входе» → нумерованные «Шаги» (подпункты — буллеты) → `bash` → `just` с
     комментариями → «UI» с URL и комментарием → «Результат / на выходе»;
   - все ссылки на файлы, разделы, ADR и URL — гиперссылки.
2. Сырые команды — без `uv run` (direnv); `just`-рецепты — отдельным блоком ниже, каждый с комментарием.
3. Нельзя:
   - менять канон dbt-проекта вне заготовленных патчей ([`scripts/demo_patch.py`](../scripts/demo_patch.py), [ADR-04](../docs/decisions.md));
   - оставлять `dbt/models` грязными после проверки (`just demo-fix`);
   - удалять `.archive/`, `.claude/.archive/`.
4. Любая команда в документах должна существовать и быть проверена прогоном; непроверенное — метка **⚠ проверить**.
5. Коммиты — маленькие, по фазам (`test(demo): …`, `docs(demo): …`, `feat(demo-prep): …`); ветку не пушить без
   просьбы пользователя.

<a id="t1"></a>
## 1. Окружение и порт 3000

1. Проверить инструменты: `uv`, `uvx`, `just`, `direnv`, `dg`, `dbt`, `mlflow`.
2. Убедиться, что Dagster слушает **3000**:
   - `.env`: `DAGSTER_PORT=3000` (если `.env` нет — `just env`);
   - [`../Justfile`](../Justfile) и [`Justfile`](Justfile) берут порт из `DAGSTER_PORT`, по умолчанию 3000.
3. Освободить порт 3000, если занят:
   - посмотреть, кто держит порт;
   - если это `dagster`/`dg dev` из этого репозитория или из `demo/olist_ml` — остановить;
   - если чужой процесс — **не убивать**, спросить пользователя.

```bash
lsof -nP -iTCP:3000 -sTCP:LISTEN                  # кто слушает порт
ps -o pid,command -p "$(lsof -ti tcp:3000)"       # чей процесс
kill "$(lsof -ti tcp:3000)"                       # только если это dagster из этого репо
lsof -nP -iTCP:3000 -sTCP:LISTEN || echo "3000 свободен"
```

4. Результат: порт 3000 свободен до старта каждого демо; во время демо его держит ровно один `dg dev`.

<a id="t2"></a>
## 2. Все тесты

1. Статические проверки и юнит-/интеграционные тесты основного проекта.
2. e2e сюжета демо 2 ([`../tests/e2e/test_demo_flow.py`](../tests/e2e/test_demo_flow.py)).
3. Смоук материалов демо 1 ([`tests/test_materials_1.py`](tests/test_materials_1.py)).
4. Любой провал — исправить причину, не тест; если причина в материалах демо — поправить и материалы, и
   соответствующий слайд.

```bash
just check                            # dbt parse + dg check defs + ruff
just test                             # smoke + unit + integration
just test-e2e                         # весь сюжет демо 2 без UI
just demo-1-test                      # ml_v1…v4 материализуются in-process; gate краснеет на 0.99
ruff check demo && ruff format --check demo
```

<a id="t3"></a>
## 3. Демо 1 по шагам

1. Подготовка по [1-1-prepare.md](docs/1-1-prepare.md), шаги P1-1…P1-4, в том числе генеральная репетиция.
2. `just demo-1-check-ready` — все пункты ✅ (⚠️ допустимо только для `just`).
3. Для каждого слайда D1-1…D1-7 из [1-2-run.md](docs/1-2-run.md) (автоматически: `just demo-1-run`):
   - выполнить блок `bash` (не `just`) дословно — так проверяются именно сырые команды;
   - до и после — `just check-demo-1-step-X-input` / `-result`;
   - сверить «Результат / на выходе» с фактом (файлы, `dg list defs`, run'ы);
   - открыть каждый URL из блока «UI» и проверить по [разделу 5](#t5);
   - расхождение → исправить слайд (команду, ожидание или URL), снять или поставить **⚠ проверить**.
4. Повторить прогон через just-блоки: `just demo-1-reset`, затем `just demo-1-run just`.
5. `just demo-1-check-complete` — все пункты ✅.
6. Прогнать сбои из [1-3-results.md R1-4](docs/1-3-results.md#r1-4) хотя бы по одному разу: воспроизвести → убедиться,
   что описанное лечение работает.
7. Сброс: `just demo-1-reset`, порт 3000 освобождён.

<a id="t4"></a>
## 4. Демо 2–4 по шагам

1. Для каждого N = 2, 3, 4 (в этом порядке — состояние переходит из демо в демо):
   - подготовка по [2-1-prepare](docs/2-1-prepare.md) / [3-1-prepare](docs/3-1-prepare.md) / [4-1-prepare](docs/4-1-prepare.md);
   - `just demo-N-check-ready` — все пункты ✅;
   - прогон всех шагов сырыми командами с проверками: `just demo-N-run`;
   - при провале шага — `just demo-run-step N X`, разбор, исправление слайда или [`tests/steps.py`](tests/steps.py);
   - проверить URL каждого слайда ([раздел 5](#t5)), для MLflow — порт 5001;
   - для слайдов с **Fallback** — один раз вызвать сбой и проверить, что fallback работает;
   - `just demo-N-check-complete` — все пункты ✅.
2. Повторить прогон через just-блоки: `just demo-4-prepare`, затем `just demo-N-run just` для N = 2, 3, 4.
3. Хронометраж: сумма с речью ≈ 12.5 + 3 + 14.5 мин; если больше — предложить срез по
   [9-3-all-results R9-3](docs/9-3-all-results.md#r9-3), не резать самостоятельно.
4. Сброс: `just demo-4-prepare`, `git status dbt/models` чистый.

<a id="t5"></a>
## 5. Аудит и исправление URL Dagster UI

1. Собрать все URL из документов демо.

```bash
grep -rhoE 'https?://(localhost|127\.0\.0\.1):[0-9]+[^) `]*' demo/*.md | sort -u
```

2. Для каждого URL при запущенном `dg dev` (порт 3000) проверить в браузере (Playwright headless из
   `/opt/pw-browsers` или Claude in Chrome):
   - страница не «Page not found» / не пустая;
   - на странице есть ожидаемое из комментария (имя ассета, вкладка Checks, Launchpad и т.п.);
   - сохранить скриншот в `demo/.screens/<slide>-<n>.png` (каталог в `.gitignore`).
3. Имена в URL сверить с фактом через GraphQL webserver'а:
   - имя code location (ожидается `olist_ml`), имена групп, джобов (`__ASSET_JOB`, `train_job`, …);

```bash
curl -s localhost:3000/graphql -H 'content-type: application/json' \
  -d '{"query":"{ workspaceOrError { ... on Workspace { locationEntries { name } } } }"}'
```

4. Особо проверить места с **⚠ проверить**:
   - параметры вкладок ассета `?view=events|checks|lineage|definition|plots`;
   - `/assets?view=freshness`, `/assets?groups=…`;
   - Launchpad `/locations/olist_ml/jobs/<job>/playground`;
   - `/automation`.
5. Исправить URL в [`demo/*.md`](README.md); если в dagster 1.13.23 прямого URL нет — написать путь кликами
   (например, «Asset → Checks») и убрать ссылку.
6. Добавить регресс-проверку: [`demo/tests/test_urls.py`](#t5) (создать) (маркер `ui`, запускается только при поднятом UI) —
   парсит URL из [`demo/*.md`](README.md) и проверяет HTTP 200 и отсутствие «Page not found» через Playwright.

<a id="t6"></a>
## 6. Отчёт

1. Итог — в [`../docs/progress.md`](../docs/progress.md): что прогнано, что исправлено, какие ⚠ сняты, хронометраж.
2. Короткая сводка пользователю: какие проверки зелёные, что осталось решить автору.

---

<a id="t7"></a>
## 7. Разработка `src/demo_prep`: scripts / configs / settings

> Имя модуля: каталог `src/demo_prep/` (Python не импортирует дефис), CLI — `demo-prep` через
> `[project.scripts]`. Цель — убрать логику из [`demo/tests/steps.py`](tests/steps.py), [`demo/materials/1/export_mart.py`](materials/1/export_mart.py) и
> [`Justfile`](Justfile)-рецептов в тестируемый пакет.

1. Структура:

```
src/demo_prep/
├── __init__.py
├── settings.py      # DemoSettings(BaseSettings): пути, порты, имена — только из .env / config
├── config.py        # загрузка config/demo.yml (чеклисты, URL, версии материалов)
├── checklist.py     # Item / report(), из demo/tests/steps.py
├── checks/          # шаги и чеклисты демо 1–4 из demo/tests/steps.py — чистые функции → Result
├── export.py        # выгрузка витрины (бывший export_mart.py)
├── project1.py      # scaffold / deps / ml-version / reset проекта demo/olist_ml
├── ports.py         # кто держит порт, безопасная остановка своего dagster
├── urls.py          # извлечение и проверка URL из demo/*.md
└── cli.py           # typer: demo-prep check ready 1 | export-mart | ml v3_checks | port free 3000 | urls check
```

2. Settings ([ADR-09](../docs/decisions.md) «никаких скаляров в коде»):
   - `DemoSettings` на `pydantic-settings`, как [`src/olist_ml/settings.py`](../src/olist_ml/settings.py);
   - переменные с префиксом `DEMO_`: `DEMO1_PROJECT_DIR`, `DEMO_MART_PARQUET`, `DEMO_DAGSTER_PORT` (по умолчанию
     `DAGSTER_PORT`), `DEMO_MLFLOW_PORT`, `DEMO_CREATE_DAGSTER_VERSION=1.13.23`;
   - дефолты совпадают с `.env.example` (добавить туда блок `# demo`), тест-сверка как
     [`tests/unit/test_settings.py`](../tests/unit/test_settings.py).
3. Config — [`config/demo.yml`](#t7) (создать):
   - чеклисты демо (пункт, функция, soft) — чтобы чеклист в md и в коде был один;
   - список URL со слайдами и ожидаемым текстом — вход для [раздела 5](#t5);
   - порядок версий [`ml_v*.py`](materials/1/ml_v1.py).
4. Scripts:
   - [`Justfile`](Justfile)-рецепты `demo-*` вызывают только `demo-prep …` (логики в рецептах нет, [ADR-15](../docs/decisions.md));
   - [`demo/tests/steps.py`](tests/steps.py) → тонкая обёртка над `demo-prep check …` / `demo-prep run …`;
   - `demo/tests/` остаётся местом тестов: [`test_demo_prep_*.py`](#t7) (создать) на чистые функции (tmp_path, без UI).
5. Качество: `ruff`, типы, docstring на русском; `just check` и `just test` зелёные; обновить
   [README](README.md) (раздел «Команды») и [`../docs/runbook.md`](../docs/runbook.md).

<a id="t8"></a>
## 8. Агентная обвязка: skills / agents / hooks / memory / инструкции

> Политика проекта: файлы скиллов не пишутся напрямую — только через `/create-skill-candidate` →
> `/create-skill` (см. [`../.claude/tasks/skill-demo-training-selfcheck.md`](../.claude/tasks/skill-demo-training-selfcheck.md)).
> Для скиллов ниже сначала пишется спецификация в [`.claude/tasks/skill-<name>.md`](../.claude/tasks/skill-demo-training-selfcheck.md) (образец).

1. Инструкции (agentic instructions):
   - [`CLAUDE.md`](#t8) (создать) в корне (сейчас его нет): назначение репо, где что лежит, правила из [раздела 0](#t0),
     команды `just check/test/demo-*`, запреты (канон dbt, чужие процессы на порту 3000);
   - [`demo/CLAUDE.md`](#t8) (создать): формат слайдов, нумерация [`{demo}-{stage}-{prepare,run,results}.md`](README.md#формат-документов), правило «каждая
     команда проверена прогоном», гиперссылки обязательны.
2. Skills (через спецификации в `.claude/tasks/`):
   - `demo-rehearsal` — разделы 3–4 этого файла: прогон демо N по шагам с проверками и отчётом;
   - `demo-url-audit` — раздел 5: сбор, проверка и правка URL;
   - `demo-training-selfcheck` — уже специфицирован, довести до скилла;
   - `demo-slide-format` — приведение нового слайда к формату (Дано / Шаги / UI / Результат, ссылки).
3. Agents ([`.claude/agents/*.md`](#t8) (создать)):
   - `demo-runner` — выполняет команды слайда и сверяет «Результат / на выходе»; инструменты: Bash, Read;
   - `ui-verifier` — Playwright/Chrome: открывает URL, делает скриншот, сравнивает с ожиданием;
   - `docs-linker` — проверяет, что все ссылки в [`demo/*.md`](README.md) резолвятся (файлы и якоря).
4. Hooks ([`.claude/settings.json`](../.claude/settings.json)):
   - `PostToolUse` на `Edit|Write` для [`demo/*.md`](README.md) → проверка ссылок и формата (скрипт `demo-prep docs lint`),
     exit 2 с сообщением при нарушении;
   - `PreToolUse` на `Bash` с `kill` / `lsof` по порту 3000 → разрешать только процессы dagster из репо;
   - `PreToolUse` на `Edit|Write` в `dbt/models/**` → запрет вне [`scripts/demo_patch.py`](../scripts/demo_patch.py);
   - `Stop` → напоминание запустить `just demo-N-check-complete`, если в сессии был прогон демо.
5. Memory:
   - устойчивые факты проекта (порт 3000, имя location `olist_ml`, модель `olist_late_delivery`, версии
     dagster/dbt/mlflow) — в [`CLAUDE.md`](#t8) (создать), не в разговоре;
   - текущее состояние — в [`../.claude/CLAUDE-curr-status.md`](../.claude/CLAUDE-curr-status.md) и
     [`../docs/progress.md`](../docs/progress.md), обновлять в конце каждой фазы;
   - журнал запросов — [`.claude/.PROMPTS-LOG.md`](../.claude/.PROMPTS-LOG.md) (дописывать, не переписывать).
6. Проверка обвязки: новая сессия Claude Code по одному [`CLAUDE.md`](#t8) (создать) + скиллу `demo-rehearsal` проходит
   разделы 1–5 без уточняющих вопросов.
