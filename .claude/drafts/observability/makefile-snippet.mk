# Цели для корневого Makefile (файл этапа 1 — вливать аддитивным diff'ом).
# Профили передаются явным списком, а не через COMPOSE_PROFILES из .env:
# профиль observability в одиночку не поднимается (зависимости в core).

COMPOSE ?= docker compose --env-file .env -f docker-compose.yml

.PHONY: docker-up docker-up-observability docker-down docker-test docker-logs

docker-up:                     ## Dagster + MLflow (демо 1-3)
	$(COMPOSE) --profile core up -d --build

docker-up-observability:       ## + Grafana / Prometheus / Loki / Alloy / exporter (демо 4)
	$(COMPOSE) --profile core --profile observability up -d --build

docker-down:                   ## Остановить всё; volume'ы остаются
	$(COMPOSE) --profile core --profile observability down

docker-test:                   ## Прогнать тесты внутри образа приложения
	$(COMPOSE) --profile core run --rm dagster-webserver pytest -q

docker-logs:                   ## Логи ядра стенда
	$(COMPOSE) --profile core logs -f dagster-webserver dagster-daemon
