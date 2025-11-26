"""Shared schema definitions for Bitcoin candle storage."""

from __future__ import annotations

from typing import Tuple

CANDLE_COLUMN_ORDER: Tuple[str, ...] = (
    "open_time",
    "close_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume_btc",
    "volume_usd",
    "trade_count",
    "taker_buy_volume_btc",
    "taker_buy_volume_usd",
    "price_increase_label",
)

CANDLE_DUCKDB_TYPES: Tuple[str, ...] = (
    "TIMESTAMP",
    "TIMESTAMP",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "DOUBLE",
    "BIGINT",
    "DOUBLE",
    "DOUBLE",
    "INTEGER",
)


def candle_row(candle, *, label: int) -> Tuple:
    """Return row data for DuckDB insertion in canonical column order."""
    values = []
    for column in CANDLE_COLUMN_ORDER:
        if column == "price_increase_label":
            values.append(label)
        else:
            values.append(getattr(candle, column))
    return tuple(values)


def duckdb_schema_sql(table: str) -> str:
    """Return CREATE TABLE statement using canonical schema."""
    column_defs = []
    for name, type_ in zip(CANDLE_COLUMN_ORDER, CANDLE_DUCKDB_TYPES, strict=True):
        if name == "open_time":
            column_defs.append(f"{name} {type_} PRIMARY KEY")
        else:
            column_defs.append(f"{name} {type_}")
    columns = ",\n                ".join(column_defs)
    return f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns}
            )
            """


def select_clause() -> str:
    """Return a comma-separated list of candle columns for SELECT statements."""
    return ",\n            ".join(CANDLE_COLUMN_ORDER)
