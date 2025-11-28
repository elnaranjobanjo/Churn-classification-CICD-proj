"""General helper utilities for reading feature store tables via DuckDB."""

from __future__ import annotations

from typing import Callable, List, Optional, Sequence, TypeVar

from .tools.binance_client import BitcoinCandle
from .tools.schemas import (
    BASE_COLUMN_NAMES,
    LABELED_COLUMN_NAMES,
    build_LabeledBitcoinCandle,
)
from .tools.singletons import get_duckdb_storage_manager

T = TypeVar("T")
RowFactory = Callable[[tuple], T]
LabeledBitcoinCandle = build_LabeledBitcoinCandle()


def load_rows_from_duckdb(
    *,
    table: str,
    columns: Sequence[str],
    limit: Optional[int] = None,
    order_by: str | None = None,
    order_desc: bool = False,
    row_factory: RowFactory | None = None,
) -> List[T] | List[tuple]:
    """Return ordered rows from the requested table."""
    if not columns:
        raise ValueError("columns must include at least one field")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    storage = get_duckdb_storage_manager()
    return storage.fetch_rows(
        table=table,
        columns=columns,
        limit=limit,
        order_by=order_by,
        order_desc=order_desc,
        row_factory=row_factory,
    )


def load_candles_from_duckdb(
    *,
    table: str = "btc_candles",
    columns: Sequence[str] | None = None,
    limit: Optional[int] = None,
    order_desc: bool = False,
) -> List[BitcoinCandle]:
    """Return Bitcoin candles stored in DuckDB as ``BitcoinCandle`` objects."""
    active_columns = list(columns or BASE_COLUMN_NAMES)
    rows = load_rows_from_duckdb(
        table=table,
        columns=active_columns,
        limit=limit,
        order_by="open_time",
        order_desc=order_desc,
        row_factory=lambda row: BitcoinCandle(*row),
    )
    return rows


def load_labeled_candles_from_duckdb(
    *,
    table: str = "btc_candles_labeled",
    columns: Sequence[str] | None = None,
    limit: Optional[int] = None,
    order_desc: bool = False,
) -> List[LabeledBitcoinCandle]:
    """Return labeled Bitcoin candles stored in DuckDB."""
    active_columns = list(columns or LABELED_COLUMN_NAMES)
    rows = load_rows_from_duckdb(
        table=table,
        columns=active_columns,
        limit=limit,
        order_by="open_time",
        order_desc=order_desc,
        row_factory=lambda row: LabeledBitcoinCandle(*row),
    )
    return rows
