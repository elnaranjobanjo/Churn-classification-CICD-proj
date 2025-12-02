"""Public API for the model training service."""

from .training import (
    TrainingResult,
    train_next_move_logistic_classifier,
)

__all__ = [
    "train_next_move_logistic_classifier",
    "TrainingResult",
]
