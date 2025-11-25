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
        ordered = sorted(candles, key=lambda c: c.open_time)
        if not ordered:
            logger.info("No candles supplied for DuckDB storage")
            return 0

        conn = duckdb.connect(str(self.db_path))
        try:
            self._ensure_schema(conn)
            last_close = self._fetch_last_close(conn)
            rows = []
            existing = self._fetch_existing_keys(conn, [c.open_time for c in ordered])
            inserted_count = 0
            for candle in ordered:
                if last_close is None:
                    label = 0
                else:
                    label = int(candle.close_price > last_close)
                last_close = candle.close_price
                rows.append(
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
                        label,
                    )
                )
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.table} VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows,
            )
            inserted_count = sum(
                1 for candle in ordered if candle.open_time not in existing
            )
        finally:
            conn.close()

        logger.info("Stored %s BTC candles into %s", inserted_count, self.db_path)
        return inserted_count

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
                taker_buy_volume_usd DOUBLE,
                price_increase_label INTEGER
            )
            """
        )
        columns = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info('{self.table}')").fetchall()
        }
        if "price_increase_label" not in columns:
            conn.execute(
                f"""
                ALTER TABLE {self.table}
                ADD COLUMN price_increase_label INTEGER DEFAULT 0
                """
            )

    def _fetch_last_close(self, conn: duckdb.DuckDBPyConnection) -> float | None:
        result = conn.execute(
            f"SELECT close_price FROM {self.table} ORDER BY open_time DESC LIMIT 1"
        ).fetchone()
        return result[0] if result else None

    def _fetch_existing_keys(
        self, conn: duckdb.DuckDBPyConnection, keys: list
    ) -> set:
        if not keys:
            return set()
        placeholders = ",".join(["?"] * len(keys))
        rows = conn.execute(
            f"SELECT open_time FROM {self.table} WHERE open_time IN ({placeholders})",
            keys,
        ).fetchall()
        return {row[0] for row in rows}
