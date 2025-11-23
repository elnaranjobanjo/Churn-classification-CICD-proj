from __future__ import annotations

import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

from MLOps.tracking import run_training_with_tracking

LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
LOG_FILE = Path("project.log")

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="MLFlow project orchestration CLI")
    subparsers = parser.add_subparsers(dest="command")

    track_parser = subparsers.add_parser(
        "track", help="Train model and log results with MLFlow"
    )
    track_parser.add_argument(
        "--experiment",
        default="default",
        help="MLFlow experiment name",
    )
    track_parser.add_argument(
        "--run-name",
        default=None,
        help="Optional MLFlow run name",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "track":
        logger.info("Executing tracked training run")
        run_training_with_tracking(args.experiment, args.run_name)
        logger.info("Tracking run finished")
        return

    logger.info("Completed run")


if __name__ == "__main__":
    main()
