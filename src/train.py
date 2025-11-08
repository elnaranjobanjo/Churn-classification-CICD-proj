from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

DATA_PATH = Path(__file__).resolve().parents[1] / "data.csv"


def load_xy(data_path: Path | str = DATA_PATH) -> Tuple[List[List[float]], List[float]]:
    """Load the toy dataset into feature/target arrays suitable for sklearn."""
    path = Path(data_path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        xs: List[List[float]] = []
        ys: List[float] = []
        for row in reader:
            xs.append([float(row["x"])])
            ys.append(float(row["y"]))
    return xs, ys


def train_model(data_path: Path | str = DATA_PATH) -> LinearRegression:
    """Fit a simple LinearRegression model on the CSV data."""
    X, y = load_xy(data_path)
    model = LinearRegression()
    model.fit(X, y)
    return model


def validate_model(
    model: LinearRegression, data_path: Path | str = DATA_PATH
) -> Dict[str, float]:
    """Compute basic regression metrics on the provided dataset."""
    X, y = load_xy(data_path)
    predictions = model.predict(X)
    return {
        "mse": float(mean_squared_error(y, predictions)),
        "r2": float(r2_score(y, predictions)),
    }


def train_and_validate(data_path: Path | str = DATA_PATH):
    """Utility helper to run the full loop in one call."""
    model = train_model(data_path)
    metrics = validate_model(model, data_path)
    return model, metrics
