"""FastAPI application scaffolding for serving model predictions."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PredictionRequest(BaseModel):
    """Payload schema for inference requests."""

    features: list[float] = Field(
        ...,
        description="Flat list of feature values expected by the model",
        min_items=1,
    )


class PredictionResponse(BaseModel):
    """Response schema for inference results."""

    prediction: Any
    raw_output: Any


@lru_cache
def load_model() -> mlflow.pyfunc.PyFuncModel:
    """Load the registered MLflow model exactly once per process."""
    model_uri = os.getenv("MODEL_URI", "models:/bitcoin-model@production")
    logger.info("Loading MLflow model from %s", model_uri)
    return mlflow.pyfunc.load_model(model_uri=model_uri)


def create_app() -> FastAPI:
    """Create and return a FastAPI app instance."""
    app = FastAPI(
        title="Bitcoin Predictor API",
        description=(
            "Serves predictions from the registered MLflow model. "
            "Set MODEL_URI to change which model version is loaded."
        ),
        version="0.1.0",
    )

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        """Basic readiness probe."""
        return {"status": "ok"}

    @app.post("/predict", response_model=PredictionResponse, tags=["inference"])
    def predict(payload: PredictionRequest) -> PredictionResponse:
        """Run inference against the loaded model."""
        try:
            model = load_model()
        except Exception as exc:  # pragma: no cover - best effort logging
            logger.exception("Unable to load MLflow model")
            raise HTTPException(status_code=503, detail="Model is unavailable") from exc

        try:
            raw_output = model.predict([payload.features])
        except Exception as exc:  # pragma: no cover - inference errors are runtime issues
            logger.exception("Inference failed")
            raise HTTPException(status_code=500, detail="Inference failed") from exc

        # Standardize the response payload for downstream consumers.
        prediction = raw_output[0] if raw_output is not None else None
        return PredictionResponse(prediction=prediction, raw_output=raw_output)

    return app


app = create_app()

