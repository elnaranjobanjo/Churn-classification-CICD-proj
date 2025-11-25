"""Single entry point for Bitcoin ingestion."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .clients import BinanceClient
from .config import load_ingestion_config
from .singletons import get_binance_client, get_duckdb_storage
from .storage import DuckDBStorage

logger = logging.getLogger(__name__)


def run_bitcoin_ingestion(
    config_path: Optional[Path] = None,
    *,
    client: Optional[BinanceClient] = None,
    storage: Optional[DuckDBStorage] = None,
) -> int:
    """Fetch BTC candles using config and persist them via DuckDB."""
    config = load_ingestion_config(config_path)
    logger.info(
        "Running BTC ingestion interval=%s limit=%s table=%s",
        config.interval,
        config.limit,
        config.table,
    )
    active_client = get_binance_client(config, client)
    active_storage = get_duckdb_storage(config, storage)
    candles = active_client.fetch_candles(
        interval=config.interval,
        limit=config.limit,
        start_time=config.start_time,
        end_time=config.end_time,
    )
    return active_storage.upsert(candles)
