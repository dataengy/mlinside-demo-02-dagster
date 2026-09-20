"""Ресурсы code location. `Settings()` строится здесь заново, чтобы тесты могли подменить пути через env."""

import dagster as dg

from olist_ml.resources import DuckDBResource, MlflowResource, PathsResource
from olist_ml.settings import Settings


@dg.definitions
def resources() -> dg.Definitions:
    s = Settings()
    return dg.Definitions(
        resources={
            "duckdb": DuckDBResource(path=str(s.DUCKDB_PATH)),
            "mlflow_res": MlflowResource(
                tracking_uri=s.MLFLOW_TRACKING_URI,
                experiment=s.MLFLOW_EXPERIMENT,
                artifacts_dir=str(s.MLFLOW_ARTIFACTS_DIR),
                ui_url=s.MLFLOW_UI_URL,
                model_name=s.MLFLOW_MODEL_NAME,
                model_alias=s.MLFLOW_MODEL_ALIAS,
            ),
            "paths": PathsResource(ml_data_dir=str(s.ML_DATA_DIR)),
        }
    )
