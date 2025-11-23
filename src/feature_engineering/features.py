"""Feature engineering utilities for the housing-price project."""

from __future__ import annotations

from typing import Tuple

import numpy as np
from sklearn.datasets import fetch_california_housing


def load_data() -> Tuple[np.ndarray, np.ndarray]:
    """Return feature matrix and target vector from the California housing dataset."""
    dataset = fetch_california_housing(as_frame=False)
    return dataset.data, dataset.target
