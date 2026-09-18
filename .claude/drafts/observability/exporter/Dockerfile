# Экспортер намеренно НЕ использует образ приложения: у него нет ни dagster,
# ни dbt, ни sklearn — только httpx и prometheus_client.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster_exporter.py .

# Порт переопределяется переменной EXPORTER_PORT (см. compose).
EXPOSE 9101
USER nobody
CMD ["python", "-u", "dagster_exporter.py"]
