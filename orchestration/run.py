from __future__ import annotations

from pathlib import Path

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

from src.train import DATA_PATH, load_xy, train_and_validate

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MLFLOW_DB = PROJECT_ROOT / "mlflow.db"
mlflow.set_tracking_uri(f"sqlite:///{MLFLOW_DB}")


def run_experiment(data_path: Path | str = DATA_PATH) -> dict[str, float]:
    """Train/validate the model while logging to MLflow."""
    features, _ = load_xy(data_path)
    input_example = [features[0]] if features else None

    with mlflow.start_run():
        mlflow.log_param("data_path", str(data_path))
        model, metrics = train_and_validate(data_path)

        for name, value in metrics.items():
            mlflow.log_metric(name, value)

        signature = (
            infer_signature(features, model.predict(features)) if features else None
        )

        mlflow.sklearn.log_model(
            model,
            name="linear-regression",
            signature=signature,
            input_example=input_example,
        )
        return metrics
