"""Shared schema definitions for Bitcoin candle storage."""

from __future__ import annotations
from dataclasses import make_dataclass
from typing import Sequence
from datetime import datetime
from typing import Tuple

# base schema from your candles as provided by binance
BASE_FIELDS = [
    ("open_time", datetime),
    ("close_time", datetime),
    ("open_price", float),
    ("high_price", float),
    ("low_price", float),
    ("close_price", float),
    ("volume_btc", float),
    ("volume_usd", float),
    ("trade_count", int),
    ("taker_buy_volume_btc", float),
    ("taker_buy_volume_usd", float),
]

# BASE_FIELDS_TYPES: Tuple[str, ...] = (
#     "TIMESTAMP",
#     "TIMESTAMP",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "BIGINT",
#     "DOUBLE",
#     "DOUBLE",
# )


def build_dataclass(
    fields: Sequence[tuple[str, type]] = None,
    name: str = "BaseCandle",
):
    """Create a dataclass with base + engineered fields."""
    return make_dataclass(
        name,
        [(field, typ) for field, typ in fields],
        bases=(),
        frozen=True,
        slots=True,
    )


def build_BitcoinCandle():
    BitcoinCandle = build_dataclass(fields=BASE_FIELDS)

    @staticmethod
    def _from_binance(payload: Sequence[str | float | int]) -> "BitcoinCandle":
        (
            open_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume_btc,
            close_time,
            volume_usd,
            trade_count,
            taker_buy_volume_btc,
            taker_buy_volume_usd,
            _ignore,
        ) = payload
        return BitcoinCandle(
            open_time=datetime.fromtimestamp(int(open_time) / 1000),
            close_time=datetime.fromtimestamp(int(close_time) / 1000),
            open_price=float(open_price),
            high_price=float(high_price),
            low_price=float(low_price),
            close_price=float(close_price),
            volume_btc=float(volume_btc),
            volume_usd=float(volume_usd),
            trade_count=int(trade_count),
            taker_buy_volume_btc=float(taker_buy_volume_btc),
            taker_buy_volume_usd=float(taker_buy_volume_usd),
        )

    BitcoinCandle.from_binance = _from_binance
    return BitcoinCandle


# if __name__ == "__main__":
#     bc = build_dataclass(fields=BASE_FIELDS)
#     for field in bc.__dataclass_fields__.values():
#         print(f"{field.name}: {field.type}")

# CANDLE_COLUMN_ORDER: Tuple[str, ...] = (
#     "open_time",
#     "close_time",
#     "open_price",
#     "high_price",
#     "low_price",
#     "close_price",
#     "volume_btc",
#     "volume_usd",
#     "trade_count",
#     "taker_buy_volume_btc",
#     "taker_buy_volume_usd",
# )

# CANDLE_DUCKDB_TYPES: Tuple[str, ...] = (
#     "TIMESTAMP",
#     "TIMESTAMP",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "DOUBLE",
#     "BIGINT",
#     "DOUBLE",
#     "DOUBLE",
# )


# def candle_row(candle) -> Tuple:
#     """Return row data for DuckDB insertion in canonical column order."""
#     values = [getattr(candle, column) for column in CANDLE_COLUMN_ORDER]
#     return tuple(values)


# def duckdb_schema_sql(table: str) -> str:
#     """Return CREATE TABLE statement using canonical schema."""
#     column_defs = []
#     for name, type_ in zip(CANDLE_COLUMN_ORDER, CANDLE_DUCKDB_TYPES, strict=True):
#         if name == "open_time":
#             column_defs.append(f"{name} {type_} PRIMARY KEY")
#         else:
#             column_defs.append(f"{name} {type_}")
#     columns = ",\n                ".join(column_defs)
#     return f"""
#             CREATE TABLE IF NOT EXISTS {table} (
#                 {columns}
#             )
#             """


# def select_clause() -> str:
#     """Return a comma-separated list of candle columns for SELECT statements."""
#     return ",\n            ".join(CANDLE_COLUMN_ORDER)
