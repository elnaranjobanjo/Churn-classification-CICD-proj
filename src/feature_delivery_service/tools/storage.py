"""Persistence helpers for Bitcoin candle features."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import duckdb

from .binance_client import BitcoinCandle
from .config import DEFAULT_FEATURE_DB_PATH
from .schemas import CANDLE_COLUMN_ORDER, candle_row, duckdb_schema_sql

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
            existing = self._fetch_existing_keys(conn, [c.open_time for c in ordered])
            rows = [candle_row(candle) for candle in ordered]
            placeholders = ", ".join(["?"] * len(CANDLE_COLUMN_ORDER))
            columns = ", ".join(CANDLE_COLUMN_ORDER)
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.table} ({columns})
                VALUES ({placeholders})
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
        conn.execute(duckdb_schema_sql(self.table))
        columns = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info('{self.table}')").fetchall()
        }
        if "price_increase_label" in columns:
            conn.execute(
                f"""
                ALTER TABLE {self.table}
                DROP COLUMN price_increase_label
                """
            )

    def _fetch_existing_keys(self, conn: duckdb.DuckDBPyConnection, keys: list) -> set:
        if not keys:
            return set()
        placeholders = ",".join(["?"] * len(keys))
        rows = conn.execute(
            f"SELECT open_time FROM {self.table} WHERE open_time IN ({placeholders})",
            keys,
        ).fetchall()
        return {row[0] for row in rows}
