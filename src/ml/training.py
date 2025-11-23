"""Model training routines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

from sklearn.metrics import mean_absolute_error, root_mean_squared_error
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from feature_engineering.features import load_data

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    model: XGBRegressor
    metrics: Dict[str, float]


def train_xgboost(
    test_size: float = 0.2,
    random_state: int = 137,
    max_depth: int = 6,
    learning_rate: float = 0.1,
    n_estimators: int = 300,
) -> TrainingResult:
    """Train an XGBoost regressor on the housing dataset."""
    features, target = load_data()
    X_train, X_test, y_train, y_test = train_test_split(
        features,
        target,
        test_size=test_size,
        random_state=random_state,
    )

    logger.info(
        "Training XGBoost model with params max_depth=%s, learning_rate=%s, n_estimators=%s",
        max_depth,
        learning_rate,
        n_estimators,
    )
    model = XGBRegressor(
        max_depth=max_depth,
        learning_rate=learning_rate,
        n_estimators=n_estimators,
        objective="reg:squarederror",
        tree_method="hist",
        n_jobs=-1,
        random_state=random_state,
    )
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)

    metrics = {"rmse": float(rmse), "mae": float(mae)}
    logger.info("XGBoost metrics: %s", metrics)
    return TrainingResult(model=model, metrics=metrics)
