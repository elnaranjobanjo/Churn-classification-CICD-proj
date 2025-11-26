"""Singleton helpers for Binance ingestion components."""

from __future__ import annotations
from .binance_client import BinanceClient

# from .storage import DuckDBStorage

# _CLIENT_CACHE: Dict[Tuple[str, str, int], BinanceClient] = {}
# _STORAGE_CACHE: Dict[Tuple[str, str], DuckDBStorage] = {}

global_binance_client = None


def get_binance_client() -> BinanceClient:
    global global_binance_client
    if global_binance_client == None:
        global_binance_client = BinanceClient()
    return global_binance_client


# def get_duckdb_storage(
#     config: IngestionConfig,
#     override: Optional[DuckDBStorage] = None,
# ) -> DuckDBStorage:
#     """Return singleton DuckDBStorage matching the config (override wins)."""
#     if override is not None:
#         return override
#     key = (str(Path(config.db_path).resolve()), config.table)
#     storage = _STORAGE_CACHE.get(key)
#     if storage is None:
#         storage = DuckDBStorage(
#             db_path=config.db_path,
#             table=config.table,
#         )
#         _STORAGE_CACHE[key] = storage
#     return storage
