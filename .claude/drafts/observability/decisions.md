# Решения лейна «Observability + Docker Compose» (черновик для docs/decisions.md)

Формат ADR-«лайт»: контекст → решение → следствие. Все утверждения о версиях
проверены по исходникам пакетов (dagster 1.13.23, dagster-graphql 1.13.23,
dagster-postgres 0.29.23, mlflow-skinny 3.16.1), а не по памяти.

## ADR-1. Метрики Dagster попадают в Prometheus через sidecar-экспортер

Рассмотрены три варианта:

- **(a) sidecar-экспортер** — опрашивает GraphQL Dagster и REST MLflow, отдаёт `/metrics`.
- **(b) метрики из логов через Loki** — `count_over_time` по структурированным логам,
  затем Loki ruler → remote-write в Prometheus.
- **(c) Grafana datasource Infinity/JSON** — дашборд ходит в GraphQL напрямую.

**Решение: (a).** У Dagster нет ни OTel-экспорта, ни эндпоинта `/metrics`;
единственная «официальная» интеграция с Prometheus — ресурс-pushgateway, то есть
push из кода ассета, а не наблюдение за оркестратором. Вариант (b) не даёт двух
из четырёх требуемых сигналов вообще: статус asset check и статус freshness в
логи не пишутся, а парсинг текста — самая хрупкая часть любого стенда. Вариант
(c) прямо противоречит ТЗ (Prometheus обязателен) и лишает алерты истории.
**Следствие:** +1 контейнер на ~150 строк Python и один источник правды для
всех панелей и всех четырёх правил алертинга.

## ADR-2. Хранилище Dagster в Compose — Postgres, не SQLite

В стенде в event log пишут одновременно три вида процессов: `dagster-webserver`,
`dagster-daemon` и каждый run-процесс, который daemon запускает подпроцессом
через `DefaultRunLauncher`. SQLite-хранилище Dagster документировано как
вариант для одного локального процесса; на общей Docker-volume (Docker Desktop,
virtiofs) многопроцессная запись регулярно даёт `database is locked`, а
диагностика этого на лекции стоит дороже, чем один сервис.
**Решение: `postgres:16-alpine` с healthcheck, зависимости через
`condition: service_healthy`.** Упрощение от SQLite было бы минимальным (минус
один сервис), а риск — несоразмерным.
**Следствие:** dev и prod отличаются ровно двумя вещами — `storage` и
`run_coordinator` в `dagster.yaml`; всё остальное едет из `.env`.

## ADR-3. Сбор логов — Grafana Alloy, а не OpenTelemetry Collector

Логи нужны от контейнеров, а не от приложения: Dagster не умеет OTLP, он пишет
в stdout. У Alloy для этого есть `discovery.docker` + `loki.source.docker` —
он читает лог-стримы прямо у демона Docker. У OTel Collector аналог —
`filelog receiver` по `/var/lib/docker/containers`, а на macOS этот путь живёт
внутри VM Docker Desktop и хосту недоступен.
**Решение: Alloy, одним контейнером, только для логов.** Метрики Prometheus
скрейпит у экспортера сам — прогонять их через `prometheus.remote_write` Alloy
означало бы лишний хоп и `--web.enable-remote-write-receiver` на Prometheus
ради одной статической цели.
**Следствие:** Alloy монтирует `/var/run/docker.sock` read-only. Это доступ
уровня root к демону Docker — приемлемо для локального демо, но в `docs/deploy/`
для Hetzner это отдельный пункт.

## ADR-4. Freshness читается из `freshnessStatusInfo`, а не вычисляется по времени

В GraphQL 1.13 у `AssetNode` есть поле `freshnessStatusInfo { freshnessStatus }`
со значениями `HEALTHY | WARNING | DEGRADED | UNKNOWN | NOT_APPLICABLE` —
это ровно тот статус, который считает демон свежести по `FreshnessPolicy`,
то есть то же самое, что видно в UI.
**Решение: основная метрика — `dagster_asset_freshness_status` (0/1/2, а
`UNKNOWN` = −1, чтобы «ещё не оценено» не попадало под порог «> 1»).**
Дополнительно экспортируется `dagster_asset_last_materialization_timestamp`
как страховка: если демон свежести выключен, алерт можно перевести на
`time() - dagster_asset_last_materialization_timestamp > N`.
**Следствие:** в `dagster.prod.yaml` обязателен `freshness: enabled: true` —
без него статус всегда `UNKNOWN` и правило не сработает никогда.

## ADR-5. JSON-логи включаются флагом CLI, переменной окружения нет

Проверено по исходникам: переменной `DAGSTER_LOG_FORMAT` в dagster 1.13.23 **не
существует**. Формат задаётся опцией `--log-format` со значениями
`colored | json | rich` у `dagster-webserver` и `dagster-daemon run`
(`dagster/_utils/log.py::configure_loggers`).
**Решение: флаг в `command:` обоих сервисов compose.** `python_logs.
dagster_handler_config` в `dagster.yaml` оставлен закомментированным: он нужен
только для JSON от логгеров пользовательского кода внутри run-процессов и
требует `python-json-logger` в образе.
**Следствие:** логи run-воркеров в Loki остаются текстовыми (они наследуют
формат code server'а); полная картина по запуску — в Dagster UI, где event log
лежит в Postgres.
