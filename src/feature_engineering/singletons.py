"""Singleton helpers for Binance ingestion components."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

from .clients import BinanceClient
from .config import IngestionConfig
from .storage import DuckDBStorage

_CLIENT_CACHE: Dict[Tuple[str, str, int], BinanceClient] = {}
_STORAGE_CACHE: Dict[Tuple[str, str], DuckDBStorage] = {}


def get_binance_client(
    config: IngestionConfig,
    override: Optional[BinanceClient] = None,
) -> BinanceClient:
    """Return singleton BinanceClient matching the config (override wins)."""
    if override is not None:
        return override
    key = (config.base_url, config.symbol, config.timeout)
    client = _CLIENT_CACHE.get(key)
    if client is None:
        client = BinanceClient(
            base_url=config.base_url,
            symbol=config.symbol,
            timeout=config.timeout,
        )
        _CLIENT_CACHE[key] = client
    return client


def get_duckdb_storage(
    config: IngestionConfig,
    override: Optional[DuckDBStorage] = None,
) -> DuckDBStorage:
    """Return singleton DuckDBStorage matching the config (override wins)."""
    if override is not None:
        return override
    key = (str(Path(config.db_path).resolve()), config.table)
    storage = _STORAGE_CACHE.get(key)
    if storage is None:
        storage = DuckDBStorage(
            db_path=config.db_path,
            table=config.table,
        )
        _STORAGE_CACHE[key] = storage
    return storage


def clear_singletons() -> None:
    """Clear caches (intended for tests)."""
    _CLIENT_CACHE.clear()
    _STORAGE_CACHE.clear()
