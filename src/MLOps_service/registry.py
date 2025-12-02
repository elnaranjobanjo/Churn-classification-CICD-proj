"""Utilities for registering tracked MLFlow runs."""

from __future__ import annotations

import logging
from typing import Optional

import mlflow

logger = logging.getLogger(__name__)


def register_run(
    run_id: str,
    model_name: str,
    alias: Optional[str] = None,
) -> str:
    """Register a tracked run's model artifacts in the MLFlow model registry."""
    model_uri = f"runs:/{run_id}/model"
    logger.info("Registering run %s as model %s from %s", run_id, model_name, model_uri)
    registered_model = mlflow.register_model(model_uri=model_uri, name=model_name)

    if alias:
        client = mlflow.MlflowClient()
        client.set_registered_model_alias(
            name=model_name,
            alias=alias,
            version=registered_model.version,
        )

    logger.info(
        "Registered run %s as model %s version %s",
        run_id,
        model_name,
        registered_model.version,
    )
    return registered_model.version
