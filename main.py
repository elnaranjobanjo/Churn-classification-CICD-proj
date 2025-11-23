from __future__ import annotations

import logging
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
LOG_FILE = Path("project.log")

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting MLFlow")
    print("Hello, world!")
    logger.info("Completed bootstrap run")


if __name__ == "__main__":
    main()
