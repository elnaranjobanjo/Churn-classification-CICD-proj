from __future__ import annotations

import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

# from MLOps.tracking import run_training_with_tracking
# from MLOps.registry import register_run
from feature_delivery_service import run_bitcoin_ingestion

# from reporting import generate_ingestion_report

LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
LOG_FILE = Path("project.log")

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Bitcoin-ML-cycle-sandbox-project orchestration CLI"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Flags reserved for fetching and storing data
    subparsers.add_parser(
        "ingest",
        help="Fetch Bitcoin candles and persist them via DuckDB",
    )

    # Flags reserved for tracking experiments.
    # track_parser = subparsers.add_parser(
    #     "track", help="Train model and log results with MLFlow"
    # )
    # track_parser.add_argument(
    #     "--experiment",
    #     default="default",
    #     help="MLFlow experiment name",
    # )
    # track_parser.add_argument(
    #     "--run-name",
    #     default=None,
    #     help="Optional MLFlow run name",
    # )

    # # Flags reserved for model registration
    # register_parser = subparsers.add_parser(
    #     "register",
    #     help="Register a tracked run's model in the MLFlow registry",
    # )
    # register_parser.add_argument("--run-id", required=True, help="Run ID to register")
    # register_parser.add_argument(
    #     "--model-name",
    #     required=True,
    #     help="Model name to use in the registry",
    # )
    # register_parser.add_argument(
    #     "--alias",
    #     default=None,
    #     help="Optional alias (e.g., staging, prod) for the registered version",
    # )

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        new_rows, total_rows = run_bitcoin_ingestion()
        logger.info("Stored %s new BTC candles (total=%s)", new_rows, total_rows)
        # report_path = generate_ingestion_report()
        # logger.info("Generated ingestion report at %s", report_path)
        return

    # if args.command == "track":
    #     logger.info("Executing tracked training run")
    #     run_training_with_tracking(args.experiment, args.run_name)
    #     logger.info("Tracking run finished")
    #     return

    # if args.command == "register":
    #     logger.info(
    #         "Registering run %s into model %s",
    #         args.run_id,
    #         args.model_name,
    #     )
    #     register_run(args.run_id, args.model_name, args.alias)
    #     logger.info("Registration finished")
    #     return


if __name__ == "__main__":
    main()
