"""Helpers to read stored Bitcoin candles from DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import duckdb

from .tools.binance_client import BitcoinCandle
from .config import DEFAULT_FEATURE_DB_PATH
from .schema import CANDLE_COLUMN_ORDER, select_clause
from .storage import _validated_identifier


def load_candles_from_duckdb(
    *,
    db_path: str | Path | None = None,
    table: str = "btc_candles",
    limit: Optional[int] = None,
    order_desc: bool = False,
) -> List[BitcoinCandle]:
    """Return Bitcoin candles stored in DuckDB as ``BitcoinCandle`` objects."""
    path = Path(db_path or DEFAULT_FEATURE_DB_PATH)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB database {path} does not exist")

    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    table_name = _validated_identifier(table)
    order_clause = "ORDER BY open_time DESC" if order_desc else "ORDER BY open_time"
    query = f"""
        SELECT
            {select_clause()}
        FROM {table_name}
        {order_clause}
    """
    if limit is not None:
        query += " LIMIT ?"

    conn = duckdb.connect(str(path))
    try:
        if limit is not None:
            rows = conn.execute(query, [limit]).fetchall()
        else:
            rows = conn.execute(query).fetchall()
    finally:
        conn.close()

    candles = [
        BitcoinCandle(
            open_time=row[0],
            close_time=row[1],
            open_price=row[2],
            high_price=row[3],
            low_price=row[4],
            close_price=row[5],
            volume_btc=row[6],
            volume_usd=row[7],
            trade_count=row[8],
            taker_buy_volume_btc=row[9],
            taker_buy_volume_usd=row[10],
        )
        for row in rows
    ]
    return candles


def count_candles(
    *,
    db_path: str | Path | None = None,
    table: str = "btc_candles",
) -> int:
    """Return the number of stored candles without loading them."""
    path = Path(db_path or DEFAULT_FEATURE_DB_PATH)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB database {path} does not exist")

    table_name = _validated_identifier(table)
    conn = duckdb.connect(str(path))
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    finally:
        conn.close()
