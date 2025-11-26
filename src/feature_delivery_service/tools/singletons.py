"""Singleton helpers for Binance ingestion components."""

from __future__ import annotations
from .binance_client import BinanceClient

from .duckdb_storage_manager import DuckDBStorageManager

global_binance_client = None
global_duckdb_storage_manager = None


def get_binance_client() -> BinanceClient:
    global global_binance_client
    if global_binance_client is None:
        global_binance_client = BinanceClient()
    return global_binance_client


def get_duckdb_storage_manager() -> DuckDBStorageManager:
    global global_duckdb_storage_manager
    if global_duckdb_storage_manager is None:
        global_duckdb_storage_manager = DuckDBStorageManager()
    return global_duckdb_storage_manager
