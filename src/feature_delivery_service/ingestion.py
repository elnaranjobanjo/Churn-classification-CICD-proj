"""Single entry point for Bitcoin ingestion."""

from __future__ import annotations

import logging
from .tools.config import load_ingestion_config
from .tools.singletons import get_binance_client, get_duckdb_storage_manager
from .tools.schemas import BASE_COLUMN_NAMES, BASE_FIELDS_TYPES

logger = logging.getLogger(__name__)


def run_bitcoin_ingestion(
    # client: Optional[BinanceClient] = None,
    # storage: Optional[DuckDBStorage] = None,
) -> None:
    """Fetch BTC candles using config and persist them via DuckDB."""
    config = load_ingestion_config()
    logger.info(
        "Running BTC ingestion interval=%s limit=%s table=%s",
        config.interval,
        config.limit,
        config.table,
    )
    active_client = get_binance_client()
    active_storage = get_duckdb_storage_manager()
    candles = active_client.fetch_candles(
        interval=config.interval,
        limit=config.limit,
        start_time=config.start_time,
        end_time=config.end_time,
    )
    new_rows = active_storage.upsert(
        table=config.table,
        columns=BASE_COLUMN_NAMES,
        types=list(BASE_FIELDS_TYPES),
        items=candles,
        sort_key="open_time",
    )
    total_rows = active_storage.count_rows(config.table)
    return new_rows, total_rows
