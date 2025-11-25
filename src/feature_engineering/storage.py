"""Persistence helpers for Bitcoin candle features."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import duckdb

from .clients import BitcoinCandle
from .config import DEFAULT_FEATURE_DB_PATH

logger = logging.getLogger(__name__)


def _validated_identifier(identifier: str) -> str:
    cleaned = identifier.replace("_", "")
    if not cleaned or not cleaned.isalnum():
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier


class DuckDBStorage:
    """DuckDB-backed storage for BTC candle features."""

    def __init__(
        self,
        db_path: str | Path = DEFAULT_FEATURE_DB_PATH,
        table: str = "btc_candles",
    ) -> None:
        self.db_path = Path(db_path)
        self.table = _validated_identifier(table)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def upsert(self, candles: Iterable[BitcoinCandle]) -> int:
        """Insert or replace candle rows into DuckDB."""
        rows = [
            (
                candle.open_time,
                candle.close_time,
                candle.open_price,
                candle.high_price,
                candle.low_price,
                candle.close_price,
                candle.volume_btc,
                candle.volume_usd,
                candle.trade_count,
                candle.taker_buy_volume_btc,
                candle.taker_buy_volume_usd,
            )
            for candle in candles
        ]
        if not rows:
            logger.info("No candles supplied for DuckDB storage")
            return 0

        conn = duckdb.connect(str(self.db_path))
        try:
            self._ensure_schema(conn)
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.table} VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows,
            )
        finally:
            conn.close()

        inserted = len(rows)
        logger.info("Stored %s BTC candles into %s", inserted, self.db_path)
        return inserted

    def _ensure_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                open_time TIMESTAMP PRIMARY KEY,
                close_time TIMESTAMP,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume_btc DOUBLE,
                volume_usd DOUBLE,
                trade_count BIGINT,
                taker_buy_volume_btc DOUBLE,
                taker_buy_volume_usd DOUBLE
            )
            """
        )
