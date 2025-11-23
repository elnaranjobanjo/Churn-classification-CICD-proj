"""Feature engineering utilities for the housing-price project."""

from __future__ import annotations

import logging
from typing import Tuple

import numpy as np
from sklearn.datasets import fetch_california_housing

logger = logging.getLogger(__name__)


def load_data() -> Tuple[np.ndarray, np.ndarray]:
    """Return feature matrix and target vector from the California housing dataset."""
    logger.info("Fetching California housing dataset (numpy arrays)")
    dataset = fetch_california_housing(as_frame=False)
    logger.debug("Dataset shapes -> features: %s, target: %s", dataset.data.shape, dataset.target.shape)
    return dataset.data, dataset.target
