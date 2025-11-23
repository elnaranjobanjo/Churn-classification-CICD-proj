"""Experiment orchestration utilities."""

from __future__ import annotations

import logging
from typing import Optional

import mlflow

from ml.training import train_xgboost

logger = logging.getLogger(__name__)


def run_training_with_tracking(
    experiment_name: str = "default",
    run_name: Optional[str] = None,
) -> None:
    """Train an XGBoost model while logging metrics to MLFlow."""
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=run_name):
        logger.info("Starting MLFlow run in experiment %s", experiment_name)
        result = train_xgboost()

        params = result.model.get_params()
        mlflow.log_params(params)

        for metric_name, value in result.metrics.items():
            mlflow.log_metric(metric_name, value)

        mlflow.xgboost.log_model(
            result.model,
            name="model",
            input_example=result.input_example,
            registered_model_name=None,
        )
        logger.info("Completed MLFlow run with metrics %s", result.metrics)
