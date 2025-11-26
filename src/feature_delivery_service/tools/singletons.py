"""Singleton helpers for Binance ingestion components."""

from __future__ import annotations

from typing import Optional

from .binance_client import BinanceClient
from .duckdb_storage_manager import DuckDBStorageManager

_binance_client: Optional[BinanceClient] = None
_duckdb_storage_manager: Optional[DuckDBStorageManager] = None


def get_binance_client() -> BinanceClient:
    """Return a lazily-instantiated Binance client."""
    global _binance_client
    if _binance_client is None:
        _binance_client = BinanceClient()
    return _binance_client


def get_duckdb_storage_manager() -> DuckDBStorageManager:
    """Return a lazily-instantiated DuckDB storage manager."""
    global _duckdb_storage_manager
    if _duckdb_storage_manager is None:
        _duckdb_storage_manager = DuckDBStorageManager()
    return _duckdb_storage_manager


def reset_singletons() -> None:
    """Reset cached singletons (useful for tests)."""
    global _binance_client, _duckdb_storage_manager
    _binance_client = None
    if _duckdb_storage_manager is not None:
        _duckdb_storage_manager.close()
        _duckdb_storage_manager = None
