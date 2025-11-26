"""Model training routines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
  from sklearn.model_selection import TimeSeriesSplit
import os
from xgboost import XGBRegressor

from data_ingestion_service.schema import CANDLE_COLUMN_ORDER
from data_ingestion_service.reader import load_candles_from_duckdb

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    model: XGBRegressor
    metrics: Dict[str, float]
    input_example: NDArray[np.float64]


def train_xgboost(val_size, limit):
    candles = load_candles_from_duckdb(limit=limit, order_desc=False)


# def train_xgboost(
#     test_size: float = 0.2,
#     random_state: int = 137,
#     max_depth: int = 6,
#     learning_rate: float = 0.1,
#     n_estimators: int = 300,
# ) -> TrainingResult:
#     """Train an XGBoost regressor on the housing dataset."""
#     features, target = load_candles_from_duckdb(
#     db_path: str | Path | None = None,
#     table: str = "btc_candles",
#     limit: Optional[int] = None,
#     order_desc: bool = False,
#     )
#     X_train, X_test, y_train, y_test = train_test_split(
#         features,
#         target,
#         test_size=test_size,
#         random_state=random_state,
#     )

#     logger.info(
#         "Training XGBoost model with params max_depth=%s, learning_rate=%s, n_estimators=%s",
#         max_depth,
#         learning_rate,
#         n_estimators,
#     )
#     model = XGBRegressor(
#         max_depth=max_depth,
#         learning_rate=learning_rate,
#         n_estimators=n_estimators,
#         objective="reg:squarederror",
#         tree_method="hist",
#         n_jobs=-1,
#         random_state=random_state,
#     )
#     model.fit(X_train, y_train)

#     predictions = model.predict(X_test)
#     rmse = root_mean_squared_error(y_test, predictions)
#     mae = mean_absolute_error(y_test, predictions)

#     metrics = {"rmse": float(rmse), "mae": float(mae)}
#     logger.info("XGBoost metrics: %s", metrics)
#     input_example = X_test[:5]
#     return TrainingResult(model=model, metrics=metrics, input_example=input_example)
