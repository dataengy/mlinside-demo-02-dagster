"""Ресурсы: DuckDB (один файл, один писатель — ADR-08), MLflow (тонкая обёртка — ADR-07), локальные пути."""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import duckdb
import mlflow


class DuckDBResource(dg.ConfigurableResource):
    path: str

    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(self.path, read_only=read_only)


class MlflowResource(dg.ConfigurableResource):
    """Tracking + registry на одном URI (SQLite по умолчанию); эксперимент создаётся при первом вызове."""

    tracking_uri: str
    experiment: str
    artifacts_dir: str
    ui_url: str
    model_name: str
    model_alias: str

    def setup(self) -> str:
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_registry_uri(self.tracking_uri)
        exp = mlflow.get_experiment_by_name(self.experiment)
        if exp is None:
            Path(self.artifacts_dir).mkdir(parents=True, exist_ok=True)
            exp_id = mlflow.create_experiment(
                self.experiment, artifact_location=Path(self.artifacts_dir).as_uri()
            )
        else:
            exp_id = exp.experiment_id
        mlflow.set_experiment(self.experiment)
        return exp_id

    def run_url(self, exp_id: str, run_id: str) -> str:
        return f"{self.ui_url}/#/experiments/{exp_id}/runs/{run_id}"

    def model_url(self) -> str:
        return f"{self.ui_url}/#/models/{self.model_name}"

    @property
    def champion_uri(self) -> str:
        return f"models:/{self.model_name}@{self.model_alias}"


class PathsResource(dg.ConfigurableResource):
    """Локальные каталоги данных ML (snapshot train/holdout)."""

    ml_data_dir: str

    def ml_dir(self) -> Path:
        p = Path(self.ml_data_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p
