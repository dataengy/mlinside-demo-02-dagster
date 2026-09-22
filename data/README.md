# data/

Локальные данные и артефакты. Каталог целиком в `.gitignore` (кроме этого файла) — всё здесь
воспроизводимо из raw-снапшота и пересоздаётся командами `just`, ничего руками не редактируется.

| Путь | Что это | Кто пишет |
|---|---|---|
| `olist.duckdb` | DuckDB: `raw` (снапшот из `dbt/seeds/raw`) → `staging` → `intermediate` → `marts` | dbt-ассеты через Dagster |
| `mlflow.db` | SQLite backend MLflow: эксперименты, run'ы, реестр моделей | MLflow, вызывается из ML-ассетов |
| `mlruns/` | MLflow artifact store: `models/m-<id>/artifacts/` — `model.pkl`, `MLmodel`, окружение (`conda.yaml`, `python_env.yaml`, `requirements.txt`), примеры входа | ассет `model` |
| `ml/` | `train_<fp>.parquet` / `holdout_<fp>.parquet`, где `<fp>` — fingerprint датасета (хеш содержимого витрины + параметров сплита); гарантирует, что метрики и quality gate считаны на тех же строках | ассет `training_dataset` |

Пути задаются в `src/olist_ml/settings.py` (`DATA_DIR`, `DUCKDB_PATH`, `MLFLOW_DB_PATH`,
`MLFLOW_ARTIFACTS_DIR`, `ML_DATA_DIR`) — не хардкодить их в коде или скриптах.

## Пересоздание и очистка

- `just seed` (через `demo-prepare`/`feature-mart`, см. `Justfile`) — наполнить raw и построить витрины.
- `just train` — обучить модель, записать в `mlruns/` и `mlflow.db`.
- `just clean raw|derived|all` — удалить данные по слоям (M5, ADR-10): `raw` только сырые таблицы,
  `derived` — производные (staging/intermediate/marts/ML-артефакты), `all` — всё.
- `just clean-build` — только кеши сборки (`dbt/target`, `.ruff_cache`, `.pytest_cache`, `__pycache__`), данные не трогает.
