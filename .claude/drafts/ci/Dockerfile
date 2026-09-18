# Один образ на два процесса (dagster-webserver и dagster-daemon) — роль и
# флаги задаёт `command:` в docker-compose.yml (docker-compose.yml — вне этого
# лейна, см. REPORT.md), не эта сборка. dbt-манифест собирается здесь же, на
# этапе build, а не при старте контейнера — решение с TODO, см. REPORT.md.

# --- builder: uv ставит зависимости и собирает manifest.json ---------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Сначала только манифесты зависимостей — слой кешируется отдельно от кода,
# пересборка исходников не тянет за собой переустановку пакетов.
# uv.lock — из `uv sync` на этапе scaffold (Этап 1); в этом лейне его нет,
# см. TODO в REPORT.md.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Теперь весь проект (см. .dockerignore — тесты/доки/данные внутрь не идут)
# и установка самого пакета тем же --frozen, без пересчёта lock-файла.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# TODO-проверить (этап реализации): dbt deps качает пакеты из сети на этапе
# build — сборка образа требует интернет; убедиться, что DBT_PROFILES_DIR
# внутри контейнера (dbt/, см. .env.example) совпадает с тем, что ждёт
# DbtProjectComponent в defs.yaml, иначе manifest.json соберётся не туда и
# рантайм всё равно перепарсит его заново при старте.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv run dbt deps --project-dir dbt --profiles-dir dbt \
 && uv run dbt parse --project-dir dbt --profiles-dir dbt --target-path dbt/target


# --- runtime: тонкий образ без uv и dev-зависимостей ------------------------
FROM python:3.12-slim-bookworm AS runtime

# Только для EXPOSE (Dockerfile не читает .env в рантайме) — значение по
# умолчанию совпадает с DAGSTER_PORT в .env.example. Реальный порт в
# контейнере всё равно приходит из .env через docker-compose.yml.
ARG DAGSTER_PORT=3111
EXPOSE ${DAGSTER_PORT}

RUN groupadd --gid 1000 app \
 && useradd --uid 1000 --gid app --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

USER app

# CMD намеренно не задан: `dagster-webserver -h 0.0.0.0 -p $DAGSTER_PORT
# -w workspace.yaml` и `dagster-daemon run` — это `command:` разных сервисов
# docker-compose.yml поверх одного и того же образа (см. Шаг 3 PROMPT.md).
