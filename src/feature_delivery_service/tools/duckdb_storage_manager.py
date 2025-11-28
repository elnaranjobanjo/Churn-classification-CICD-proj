"""Persistence helpers for Bitcoin candle features."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import duckdb

# from .schemas import CANDLE_COLUMN_ORDER, candle_row, duckdb_schema_sql

logger = logging.getLogger(__name__)

DEFAULT_FEATURE_DB_PATH = Path(
    os.getenv("FEATURE_DB_PATH", "feature_store/bitcoin.duckdb")
)


class DuckDBStorageManager:
    """DuckDB-backed storage for BTC candle features."""

    def __init__(self, db_path: str | Path = DEFAULT_FEATURE_DB_PATH) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))

    def upsert(
        self,
        table: str,
        columns: Sequence[str],
        types: Sequence[str],
        items: Iterable,
        sort_key: str,
    ) -> int:
        """Insert or replace candle rows into DuckDB."""

        table = self._validated_identifier(table)
        ordered = sorted(items, key=lambda c: getattr(c, sort_key))
        if not ordered:
            logger.info("No candles supplied for DuckDB storage")
            return 0

        self._ensure_schema(self.duckdb_create_table_statement(columns, types, table))
        existing = self._fetch_existing_keys(
            table, [getattr(c, sort_key) for c in ordered], sort_key
        )
        rows = [self.row(item, columns) for item in ordered]
        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join(columns)
        self.conn.executemany(
            f"""
            INSERT OR REPLACE INTO {table} ({columns_str})
            VALUES ({placeholders})
            """,
            rows,
        )
        inserted_count = sum(
            1 for candle in ordered if getattr(candle, sort_key) not in existing
        )

        logger.info("Stored %s rows into %s", inserted_count, self.db_path)
        return inserted_count

    def fetch_rows(
        self,
        table: str,
        columns: Sequence[str],
        *,
        limit: int | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        row_factory: Callable[[tuple], Any] | None = None,
    ) -> list[Any]:
        """Return ordered rows from a table, optionally applying a row factory."""
        if not columns:
            raise ValueError("columns must include at least one field")

        table = self._validated_identifier(table)
        order_column = (
            self._validated_identifier(order_by)
            if order_by
            else self._validated_identifier(columns[0])
        )
        column_clause = ", ".join(columns)
        order_clause = "DESC" if order_desc else "ASC"
        query = f"""
            SELECT {column_clause}
            FROM {table}
            ORDER BY {order_column} {order_clause}
        """
        params: list[int] = []
        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be positive when provided")
            query += " LIMIT ?"
            params.append(limit)

        rows = self.conn.execute(query, params).fetchall()
        if row_factory is None:
            return rows
        return [row_factory(row) for row in rows]

    def _ensure_schema(self, schema: str) -> None:
        self.conn.execute(schema)

    def _fetch_existing_keys(
        self,
        table: str,
        keys: Sequence,
        sort_key: str,
    ) -> set:
        if not keys:
            return set()
        placeholders = ",".join(["?"] * len(keys))
        rows = self.conn.execute(
            f"SELECT {sort_key} FROM {table} WHERE {sort_key} IN ({placeholders})",
            keys,
        ).fetchall()
        return {row[0] for row in rows}

    def count_rows(self, table: str) -> int:
        """Return the number of rows stored in the given table."""
        table = self._validated_identifier(table)
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(result[0] if result else 0)

    @staticmethod
    def duckdb_create_table_statement(
        columns: Sequence[str],
        types: Sequence[str],
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
    def row(item, columns: Sequence[str]) -> tuple:
        return tuple(getattr(item, column) for column in columns)

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.close()
            self.conn = None

    def __del__(self) -> None:
        self.close()
