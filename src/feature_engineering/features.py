"""Feature engineering utilities for the housing-price project."""

from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
from numpy.typing import NDArray
from sklearn.datasets import fetch_california_housing

logger = logging.getLogger(__name__)


def load_data() -> Tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Return feature matrix and target vector from the California housing dataset."""
    logger.info("Fetching California housing dataset (numpy arrays)")
    features, target = fetch_california_housing(as_frame=False, return_X_y=True)
    logger.debug(
        "Dataset shapes -> features: %s, target: %s", features.shape, target.shape
    )
    return features, target
