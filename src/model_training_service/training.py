"""Simple training routine for predicting the next close-price move."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Sequence

import numpy as np
from numpy.typing import NDArray
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from feature_delivery_service import load_labeled_candles_from_duckdb

logger = logging.getLogger(__name__)

FEATURE_COLUMNS: Sequence[str] = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume_btc",
    "volume_usd",
    "trade_count",
    "taker_buy_volume_btc",
    "taker_buy_volume_usd",
    "close_price_gt_prev",
)
TARGET_COLUMN = "next_close_price_gt_curr"


@dataclass
class TrainingResult:
    """Container for the trained model and evaluation metadata."""

    model: Pipeline
    metrics: Dict[str, float]
    feature_names: Sequence[str]
    input_example: NDArray[np.float64]


def _candles_to_arrays(
    candles,
) -> tuple[NDArray[np.float64], NDArray[np.int8]]:
    features = np.array(
        [
            [
                candle.open_price,
                candle.high_price,
                candle.low_price,
                candle.close_price,
                candle.volume_btc,
                candle.volume_usd,
                candle.trade_count,
                candle.taker_buy_volume_btc,
                candle.taker_buy_volume_usd,
                candle.close_price_gt_prev,
            ]
            for candle in candles
        ],
        dtype=np.float64,
    )
    labels = np.array(
        [candle.next_close_price_gt_curr for candle in candles],
        dtype=np.int8,
    )
    return features, labels


def train_next_move_logistic_classifier(
    *,
    limit: int | None = 5000,
    test_size: float = 0.2,
    random_state: int = 137,
) -> TrainingResult:
    """Fit a basic classifier to predict if the next close price increases."""
    candles = load_labeled_candles_from_duckdb(limit=limit, order_desc=False)
    if len(candles) < 100:
        raise RuntimeError(
            "Not enough labeled candles to train a classifier (need >= 100 rows)"
        )

    features, labels = _candles_to_arrays(candles)
    X_train, X_test, y_train, y_test = train_test_split(
        features,
        labels,
        test_size=test_size,
        random_state=random_state,
        stratify=labels if len(np.unique(labels)) > 1 else None,
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=500,
                    random_state=random_state,
                ),
            ),
        ]
    )
    logger.info(
        "Training logistic regression classifier on %s samples", X_train.shape[0]
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    metrics: Dict[str, float] = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
    }
    if len(np.unique(y_test)) > 1:
        y_proba = model.predict_proba(X_test)[:, 1]
        metrics["roc_auc"] = float(roc_auc_score(y_test, y_proba))

    return TrainingResult(
        model=model,
        metrics=metrics,
        feature_names=FEATURE_COLUMNS,
        input_example=X_test[:5],
    )
