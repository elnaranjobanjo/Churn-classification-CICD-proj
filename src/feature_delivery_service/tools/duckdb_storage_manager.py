"""Persistence helpers for Bitcoin candle features."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable

import duckdb

# from .schemas import CANDLE_COLUMN_ORDER, candle_row, duckdb_schema_sql

logger = logging.getLogger(__name__)

DEFAULT_FEATURE_DB_PATH = Path(
    os.getenv("FEATURE_DB_PATH", "feature_store/bitcoin.duckdb")
)


class DuckDBStorageManager:
    """DuckDB-backed storage for BTC candle features."""

    def __init__(
        self,
        db_path: str | Path = DEFAULT_FEATURE_DB_PATH,
    ) -> None:
        self.db_path = db_path
        self.conn = duckdb.connect(str(Path(db_path)))

        if db_path.parent and not db_path.parent.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)

    def upsert(
        self,
        table: str,
        columns: list[str],
        types: list[str],
        items: Iterable,
        sort_key: str,
    ) -> int:
        """Insert or replace candle rows into DuckDB."""

        table = self._validated_identifier(table)
        ordered = sorted(items, key=lambda c: getattr(c, sort_key))
        if not ordered:
            logger.info("No candles supplied for DuckDB storage")
            return 0

        conn = duckdb.connect(str(self.db_path))
        try:
            self._ensure_schema(
                self.duckdb_create_table_statement(columns, types, table)
            )
            existing = self._fetch_existing_keys(conn, [c.open_time for c in ordered])
            rows = [self.row(item) for item in ordered]
            placeholders = ", ".join(["?"] * len(columns))
            columns_str = ", ".join(columns)
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {table} ({columns_str})
                VALUES ({placeholders})
                """,
                rows,
            )
            inserted_count = sum(
                1 for candle in ordered if candle.open_time not in existing
            )
        finally:
            self.conn.close()

        logger.info("Stored %s BTC candles into %s", inserted_count, self.db_path)
        return inserted_count

    def _ensure_schema(self, schema: str) -> None:
        self.conn.execute(schema)

    def _fetch_existing_keys(self, table: str, keys: list, sort_key: str) -> set:
        if not keys:
            return set()
        placeholders = ",".join(["?"] * len(keys))
        rows = self.conn.execute(
            f"SELECT {sort_key} FROM {table} WHERE {sort_key} IN ({placeholders})",
            keys,
        ).fetchall()
        return {row[0] for row in rows}

    @staticmethod
    def duckdb_create_table_statement(
        columns: list[str],
        types: list[str],
        table: str,
    ) -> str:
        """Return CREATE TABLE statement using provided schema."""
        column_defs = []
        for name, dtype in zip(columns, types, strict=True):
            if name == columns[0]:
                column_defs.append(f"{name} {dtype} PRIMARY KEY")
            else:
                column_defs.append(f"{name} {dtype}")
        joined_columns = ",\n                ".join(column_defs)
        return f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {joined_columns}
                )
                """

    @staticmethod
    def _validated_identifier(identifier: str) -> str:
        cleaned = identifier.replace("_", "")
        if not cleaned or not cleaned.isalnum():
            raise ValueError(f"Invalid identifier: {identifier}")
        return identifier

    @staticmethod
    def row(item, columns):
        return tuple([getattr(item, column) for column in columns])
