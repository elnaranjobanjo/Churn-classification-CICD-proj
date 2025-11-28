"""ETL helpers for preparing modeling datasets."""

from __future__ import annotations

from dataclasses import asdict
from typing import List, Sequence

from .reader import load_candles_from_duckdb
from .tools.schemas import (
    LABELED_COLUMN_NAMES,
    LABELED_FIELD_TYPES,
    build_LabeledBitcoinCandle,
)
from .tools.singletons import get_duckdb_storage_manager

LabeledBitcoinCandle = build_LabeledBitcoinCandle()


def build_labeled_candles(
    *,
    table: str = "btc_candles",
    limit: int | None = None,
) -> List[LabeledBitcoinCandle]:
    """Return BTC candles augmented with lag/lead close-price indicators."""
    candles = load_candles_from_duckdb(table=table, limit=limit, order_desc=False)
    if len(candles) < 3:
        raise RuntimeError("Need at least 3 candles to build labeled dataset")

    labeled: list[LabeledBitcoinCandle] = []
    prev_iter: Sequence = candles[:-2]
    curr_iter: Sequence = candles[1:-1]
    next_iter: Sequence = candles[2:]
    for prev_candle, curr_candle, next_candle in zip(prev_iter, curr_iter, next_iter):
        payload = asdict(curr_candle)
        payload["close_price_gt_prev"] = int(
            curr_candle.close_price > prev_candle.close_price
        )
        payload["next_close_price_gt_curr"] = int(
            next_candle.close_price > curr_candle.close_price
        )
        labeled.append(LabeledBitcoinCandle(**payload))
    return labeled


def materialize_labeled_candles(
    *,
    source_table: str = "btc_candles",
    destination_table: str = "btc_candles_labeled",
    limit: int | None = None,
) -> int:
    """Persist labeled candles into DuckDB via the storage manager."""
    labeled = build_labeled_candles(table=source_table, limit=limit)
    storage = get_duckdb_storage_manager()
    inserted = storage.upsert(
        table=destination_table,
        columns=LABELED_COLUMN_NAMES,
        types=LABELED_FIELD_TYPES,
        items=labeled,
        sort_key="open_time",
    )
    return inserted
